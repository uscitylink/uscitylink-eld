import os
import time
import hashlib
import requests
from datetime import datetime

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import NullPool


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/app", StaticFiles(directory="."), name="app")


# ---------------- DATABASE ----------------

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./eld.db")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if DATABASE_URL.startswith("sqlite"):
    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False},
        poolclass=NullPool
    )
else:
    engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


# ---------------- HELPERS ----------------

def now_utc():
    return datetime.utcnow().isoformat()


def today_utc():
    return datetime.utcnow().date().isoformat()


def create_salt():
    return os.urandom(16).hex()


def hash_password(password: str, salt: str):
    return hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        100000
    ).hex()


def get_event_code(duty_status):
    mapping = {
        "OFF": "1",
        "SB": "2",
        "DRIVING": "3",
        "ON": "4"
    }
    return mapping.get(duty_status.upper(), "0")


# ---------------- TABLES ----------------

class Driver(Base):
    __tablename__ = "drivers"

    id = Column(Integer, primary_key=True, index=True)
    driver_id = Column(String, unique=True)
    name = Column(String)
    license_number = Column(String)
    active = Column(String, default="YES")


class Vehicle(Base):
    __tablename__ = "vehicles"

    id = Column(Integer, primary_key=True, index=True)
    vehicle_id = Column(String, unique=True)
    vin = Column(String)
    plate = Column(String)
    active = Column(String, default="YES")


class UserAccount(Base):
    __tablename__ = "user_accounts"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True)
    password_hash = Column(String)
    password_salt = Column(String)
    role = Column(String)
    driver_id = Column(String, default="")
    active = Column(String, default="YES")


class ELDEvent(Base):
    __tablename__ = "eld_events"

    id = Column(Integer, primary_key=True, index=True)
    driver_id = Column(String)
    vehicle_id = Column(String)
    duty_status = Column(String)
    event_time = Column(String)
    latitude = Column(String)
    longitude = Column(String)
    odometer = Column(String)
    engine_hours = Column(String)
    speed = Column(Float, default=0)
    event_origin = Column(String, default="DRIVER")


class ELDEventEdit(Base):
    __tablename__ = "eld_event_edits"

    id = Column(Integer, primary_key=True, index=True)
    original_event_id = Column(Integer)
    new_event_id = Column(Integer)
    driver_id = Column(String)
    old_status = Column(String)
    new_status = Column(String)
    reason = Column(String)
    edited_at = Column(String)


class DriverCertification(Base):
    __tablename__ = "driver_certifications"

    id = Column(Integer, primary_key=True, index=True)
    driver_id = Column(String)
    log_date = Column(String)
    certified_at = Column(String)
    note = Column(String)


class DriverLog(Base):
    __tablename__ = "driver_logs"

    id = Column(Integer, primary_key=True, index=True)
    driver_id = Column(String)
    action = Column(String)
    timestamp = Column(String)


class EngineEvent(Base):
    __tablename__ = "engine_events"

    id = Column(Integer, primary_key=True, index=True)
    vehicle_id = Column(String)
    status = Column(String)
    timestamp = Column(String)


class Malfunction(Base):
    __tablename__ = "malfunctions"

    id = Column(Integer, primary_key=True, index=True)
    type = Column(String)
    description = Column(String)
    timestamp = Column(String)


class UnassignedEvent(Base):
    __tablename__ = "unassigned_events"

    id = Column(Integer, primary_key=True, index=True)
    vehicle_id = Column(String)
    event_time = Column(String)
    latitude = Column(String)
    longitude = Column(String)
    speed = Column(Float)
    assigned = Column(String, default="NO")
    assigned_driver = Column(String, default="")

class VehicleAssignment(Base):
    __tablename__ = "vehicle_assignments"

    id = Column(Integer, primary_key=True, index=True)
    driver_id = Column(String)
    vehicle_id = Column(String)
    assigned_at = Column(String)
    active = Column(String, default="YES")


Base.metadata.create_all(bind=engine)


# ---------------- HOME ----------------

@app.get("/")
def home():
    return {"status": "US Citylink ELD backend running"}


# ---------------- AUTH ----------------

@app.post("/api/auth/create-user")
def create_user(username: str, password: str, role: str, driver_id: str = ""):
    role = role.upper()

    if role not in ["DRIVER", "ADMIN", "DISPATCHER"]:
        return {"error": "Role must be DRIVER, ADMIN, or DISPATCHER"}

    db = SessionLocal()

    existing = db.query(UserAccount).filter(UserAccount.username == username).first()

    if existing:
        db.close()
        return {"error": "Username already exists"}

    salt = create_salt()
    password_hash = hash_password(password, salt)

    user = UserAccount(
        username=username,
        password_hash=password_hash,
        password_salt=salt,
        role=role,
        driver_id=driver_id,
        active="YES"
    )

    db.add(user)
    db.commit()
    db.close()

    return {
        "message": "User created securely",
        "username": username,
        "role": role,
        "driver_id": driver_id
    }


@app.post("/api/auth/login")
def login(username: str, password: str):
    db = SessionLocal()

    user = (
        db.query(UserAccount)
        .filter(UserAccount.username == username)
        .filter(UserAccount.active == "YES")
        .first()
    )

    if not user:
        db.close()
        return {"error": "Invalid username or password"}

    test_hash = hash_password(password, user.password_salt)

    if test_hash != user.password_hash:
        db.close()
        return {"error": "Invalid username or password"}

    db.close()

    return {
        "message": "Login successful",
        "username": user.username,
        "role": user.role,
        "driver_id": user.driver_id
    }


@app.get("/api/auth/users")
def get_users():
    db = SessionLocal()
    users = db.query(UserAccount).order_by(UserAccount.id.desc()).all()
    db.close()

    safe_users = []

    for u in users:
        safe_users.append({
            "id": u.id,
            "username": u.username,
            "role": u.role,
            "driver_id": u.driver_id,
            "active": u.active
        })

    return safe_users


# ---------------- ADMIN ----------------

@app.post("/api/admin/driver")
def create_driver(driver_id: str, name: str, license_number: str = ""):
    db = SessionLocal()

    existing = db.query(Driver).filter(Driver.driver_id == driver_id).first()

    if existing:
        db.close()
        return {"error": "Driver already exists"}

    driver = Driver(
        driver_id=driver_id,
        name=name,
        license_number=license_number,
        active="YES"
    )

    db.add(driver)
    db.commit()
    db.close()

    return {"message": "Driver created", "driver_id": driver_id}


@app.get("/api/admin/drivers")
def get_drivers():
    db = SessionLocal()
    drivers = db.query(Driver).order_by(Driver.id.desc()).all()
    db.close()
    return drivers


@app.post("/api/admin/vehicle")
def create_vehicle(vehicle_id: str, vin: str = "", plate: str = ""):
    db = SessionLocal()

    existing = db.query(Vehicle).filter(Vehicle.vehicle_id == vehicle_id).first()

    if existing:
        db.close()
        return {"error": "Vehicle already exists"}

    vehicle = Vehicle(
        vehicle_id=vehicle_id,
        vin=vin,
        plate=plate,
        active="YES"
    )

    db.add(vehicle)
    db.commit()
    db.close()

    return {"message": "Vehicle created", "vehicle_id": vehicle_id}


@app.get("/api/admin/vehicles")
def get_vehicles():
    db = SessionLocal()
    vehicles = db.query(Vehicle).order_by(Vehicle.id.desc()).all()
    db.close()
    return vehicles


# ---------------- ELD EVENTS ----------------

@app.post("/api/eld/event")
def add_event(
    driver_id: str,
    vehicle_id: str,
    duty_status: str,
    latitude: str = "",
    longitude: str = "",
    odometer: str = "",
    engine_hours: str = ""
):
    duty_status = duty_status.upper()

    if duty_status == "DRIVING":
        return {
            "error": "DRIVING cannot be selected manually. Use speed telemetry above 5 mph."
        }

    db = SessionLocal()

    event = ELDEvent(
        driver_id=driver_id,
        vehicle_id=vehicle_id,
        duty_status=duty_status,
        event_time=now_utc(),
        latitude=latitude,
        longitude=longitude,
        odometer=odometer,
        engine_hours=engine_hours,
        speed=0,
        event_origin="DRIVER"
    )

    db.add(event)
    db.commit()
    db.close()

    return {"message": f"{duty_status} event saved"}


@app.post("/api/vehicle/telemetry")
def vehicle_telemetry(
    driver_id: str = "",
    vehicle_id: str = "",
    speed: float = 0,
    latitude: str = "",
    longitude: str = "",
    odometer: str = "",
    engine_hours: str = ""
):
    db = SessionLocal()

    if speed > 5:

        if not driver_id or driver_id.strip() == "":
            unassigned = UnassignedEvent(
                vehicle_id=vehicle_id,
                event_time=now_utc(),
                latitude=latitude,
                longitude=longitude,
                speed=speed,
                assigned="NO",
                assigned_driver=""
            )

            db.add(unassigned)
            db.commit()
            db.close()

            return {
                "message": "UNASSIGNED DRIVING recorded",
                "status": "UNASSIGNED"
            }

        last_event = (
            db.query(ELDEvent)
            .filter(ELDEvent.driver_id == driver_id)
            .order_by(ELDEvent.id.desc())
            .first()
        )

        if not last_event or last_event.duty_status != "DRIVING":
            event = ELDEvent(
                driver_id=driver_id,
                vehicle_id=vehicle_id,
                duty_status="DRIVING",
                event_time=now_utc(),
                latitude=latitude,
                longitude=longitude,
                odometer=odometer,
                engine_hours=engine_hours,
                speed=speed,
                event_origin="AUTO"
            )

            db.add(event)
            db.commit()
            db.close()

            return {
                "message": "AUTO DRIVING event created",
                "speed": speed,
                "status": "DRIVING"
            }

        db.close()
        return {
            "message": "Already in DRIVING status",
            "speed": speed,
            "status": "DRIVING"
        }

    db.close()
    return {
        "message": "Telemetry received. Speed is 5 mph or less.",
        "speed": speed
    }


@app.get("/api/eld/events")
def get_events(driver_id: str = ""):
    db = SessionLocal()

    query = db.query(ELDEvent)

    if driver_id:
        query = query.filter(ELDEvent.driver_id == driver_id)

    events = query.order_by(ELDEvent.event_time).all()
    db.close()

    return events


# ---------------- CURRENT STATUS / HOS ----------------

@app.get("/api/driver/current-status")
def current_status(driver_id: str):
    db = SessionLocal()

    last_event = (
        db.query(ELDEvent)
        .filter(ELDEvent.driver_id == driver_id)
        .order_by(ELDEvent.id.desc())
        .first()
    )

    db.close()

    if not last_event:
        return {
            "driver_id": driver_id,
            "current_status": "NO EVENTS",
            "message": "No duty status found"
        }

    return {
        "driver_id": driver_id,
        "vehicle_id": last_event.vehicle_id,
        "current_status": last_event.duty_status,
        "event_time": last_event.event_time,
        "odometer": last_event.odometer,
        "engine_hours": last_event.engine_hours,
        "speed": last_event.speed,
        "event_origin": last_event.event_origin,
        "latitude": last_event.latitude,
        "longitude": last_event.longitude
    }


@app.get("/api/hos/summary")
def hos_summary(driver_id: str):
    db = SessionLocal()

    events = (
        db.query(ELDEvent)
        .filter(ELDEvent.driver_id == driver_id)
        .order_by(ELDEvent.event_time)
        .all()
    )

    db.close()

    driving_seconds = 0
    shift_seconds = 0

    last_time = None
    last_status = None

    for e in events:
        current_time = datetime.fromisoformat(e.event_time)

        if last_time:
            diff = (current_time - last_time).total_seconds()

            if last_status == "DRIVING":
                driving_seconds += diff

            if last_status in ["DRIVING", "ON"]:
                shift_seconds += diff

        last_time = current_time
        last_status = e.duty_status

    driving_hours = driving_seconds / 3600
    shift_hours = shift_seconds / 3600

    violation = driving_hours > 11 or shift_hours > 14

    if driving_hours > 11:
        violation_message = "Driving limit exceeded"
    elif shift_hours > 14:
        violation_message = "Shift limit exceeded"
    else:
        violation_message = ""

    return {
        "driver_id": driver_id,
        "driving_hours": round(driving_hours, 2),
        "shift_hours": round(shift_hours, 2),
        "driving_remaining": round(max(0, 11 - driving_hours), 2),
        "shift_remaining": round(max(0, 14 - shift_hours), 2),
        "driving_limit_hours": 11,
        "shift_limit_hours": 14,
        "violation": violation,
        "violation_message": violation_message
    }


# ---------------- DRIVER LOGIN LOGS ----------------

@app.post("/api/driver/login")
def driver_login(driver_id: str):
    db = SessionLocal()

    log = DriverLog(
        driver_id=driver_id,
        action="LOGIN",
        timestamp=now_utc()
    )

    db.add(log)
    db.commit()
    db.close()

    return {"message": "driver logged in"}


@app.post("/api/driver/logout")
def driver_logout(driver_id: str):
    db = SessionLocal()

    log = DriverLog(
        driver_id=driver_id,
        action="LOGOUT",
        timestamp=now_utc()
    )

    db.add(log)
    db.commit()
    db.close()

    return {"message": "driver logged out"}


# ---------------- UNASSIGNED ----------------

@app.get("/api/unassigned")
def get_unassigned():
    db = SessionLocal()
    data = db.query(UnassignedEvent).order_by(UnassignedEvent.event_time.desc()).all()
    db.close()
    return data


@app.post("/api/unassigned/assign")
def assign_unassigned(event_id: int, driver_id: str):
    db = SessionLocal()

    event = db.query(UnassignedEvent).filter(UnassignedEvent.id == event_id).first()

    if not event:
        db.close()
        return {"error": "Unassigned event not found"}

    event.assigned = "YES"
    event.assigned_driver = driver_id

    new_event = ELDEvent(
        driver_id=driver_id,
        vehicle_id=event.vehicle_id,
        duty_status="DRIVING",
        event_time=event.event_time,
        latitude=event.latitude,
        longitude=event.longitude,
        odometer="",
        engine_hours="",
        speed=event.speed,
        event_origin="ASSIGNED"
    )

    db.add(new_event)
    db.commit()
    db.close()

    return {"message": "Unassigned driving assigned to driver"}


# ---------------- EDITS ----------------

@app.post("/api/eld/edit-event")
def edit_event(event_id: int, new_status: str, reason: str):
    new_status = new_status.upper()

    if new_status not in ["OFF", "SB", "ON"]:
        return {"error": "Only OFF, SB, or ON are allowed for manual edits."}

    if reason.strip() == "":
        return {"error": "Edit reason is required."}

    db = SessionLocal()

    original = db.query(ELDEvent).filter(ELDEvent.id == event_id).first()

    if not original:
        db.close()
        return {"error": "Original event not found."}

    if original.duty_status == "DRIVING":
        db.close()
        return {"error": "DRIVING events cannot be edited."}

    new_event = ELDEvent(
        driver_id=original.driver_id,
        vehicle_id=original.vehicle_id,
        duty_status=new_status,
        event_time=now_utc(),
        latitude=original.latitude,
        longitude=original.longitude,
        odometer=original.odometer,
        engine_hours=original.engine_hours,
        speed=0,
        event_origin="EDIT"
    )

    db.add(new_event)
    db.commit()
    db.refresh(new_event)

    edit_record = ELDEventEdit(
        original_event_id=original.id,
        new_event_id=new_event.id,
        driver_id=original.driver_id,
        old_status=original.duty_status,
        new_status=new_status,
        reason=reason,
        edited_at=now_utc()
    )

    db.add(edit_record)
    db.commit()
    db.close()

    return {
        "message": "Event edited. Original record preserved.",
        "original_event_id": event_id,
        "new_event_id": new_event.id,
        "old_status": original.duty_status,
        "new_status": new_status,
        "reason": reason
    }


@app.get("/api/eld/edits")
def get_edits(driver_id: str = ""):
    db = SessionLocal()

    query = db.query(ELDEventEdit)

    if driver_id:
        query = query.filter(ELDEventEdit.driver_id == driver_id)

    edits = query.order_by(ELDEventEdit.edited_at.desc()).all()
    db.close()

    return edits


# ---------------- CERTIFICATION ----------------

@app.post("/api/eld/certify")
def certify_log(
    driver_id: str,
    log_date: str = "",
    note: str = "Driver certified log as true and correct"
):
    if not log_date:
        log_date = today_utc()

    db = SessionLocal()

    cert = DriverCertification(
        driver_id=driver_id,
        log_date=log_date,
        certified_at=now_utc(),
        note=note
    )

    db.add(cert)
    db.commit()
    db.close()

    return {
        "message": "Driver log certified",
        "driver_id": driver_id,
        "log_date": log_date,
        "note": note
    }


@app.get("/api/eld/certifications")
def get_certifications(driver_id: str = ""):
    db = SessionLocal()

    query = db.query(DriverCertification)

    if driver_id:
        query = query.filter(DriverCertification.driver_id == driver_id)

    certs = query.order_by(DriverCertification.certified_at.desc()).all()
    db.close()

    return certs


# ---------------- ENGINE / MALFUNCTION ----------------

@app.post("/api/engine")
def engine_event(vehicle_id: str, status: str):
    db = SessionLocal()

    event = EngineEvent(
        vehicle_id=vehicle_id,
        status=status.upper(),
        timestamp=now_utc()
    )

    db.add(event)
    db.commit()
    db.close()

    return {"message": "engine event saved"}


@app.post("/api/malfunction")
def add_malfunction(type: str, description: str):
    db = SessionLocal()

    m = Malfunction(
        type=type.upper(),
        description=description,
        timestamp=now_utc()
    )

    db.add(m)
    db.commit()
    db.close()

    return {"message": "malfunction recorded"}


# ---------------- FLEET MAP LOCAL DB ----------------

@app.get("/api/fleet/locations")
def fleet_locations():
    db = SessionLocal()

    events = db.query(ELDEvent).order_by(ELDEvent.event_time.desc()).all()
    db.close()

    latest_by_vehicle = {}

    for e in events:
        if not e.vehicle_id:
            continue

        if e.vehicle_id not in latest_by_vehicle:
            latest_by_vehicle[e.vehicle_id] = {
                "vehicle_id": e.vehicle_id,
                "driver_id": e.driver_id,
                "status": e.duty_status,
                "event_time": e.event_time,
                "latitude": e.latitude,
                "longitude": e.longitude,
                "speed": e.speed,
                "origin": e.event_origin
            }

    return list(latest_by_vehicle.values())


@app.get("/api/fleet/route")
def fleet_route(vehicle_id: str):
    db = SessionLocal()

    events = (
        db.query(ELDEvent)
        .filter(ELDEvent.vehicle_id == vehicle_id)
        .order_by(ELDEvent.event_time)
        .all()
    )

    db.close()

    route = []

    for e in events:
        route.append({
            "id": e.id,
            "vehicle_id": e.vehicle_id,
            "driver_id": e.driver_id,
            "status": e.duty_status,
            "event_time": e.event_time,
            "latitude": e.latitude,
            "longitude": e.longitude,
            "speed": e.speed,
            "origin": e.event_origin
        })

    return route


# ---------------- SAMSARA GPS ----------------

@app.get("/api/samsara/gps")
def get_samsara_gps():
    token = os.environ.get("SAMSARA_API_TOKEN")

    if not token:
        return {"error": "SAMSARA_API_TOKEN not set"}

    url = "https://api.samsara.com/fleet/vehicles/stats"

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    params = {
        "types": "gps"
    }

    try:
        res = requests.get(url, headers=headers, params=params, timeout=20)

        if res.status_code != 200:
            return {
                "error": "Samsara API error",
                "status_code": res.status_code,
                "response": res.text
            }

        data = res.json()
        trucks = []

        for vehicle in data.get("data", []):
            gps = vehicle.get("gps")

            if not gps:
                continue

            trucks.append({
                "vehicle_id": vehicle.get("name") or vehicle.get("id"),
                "latitude": gps.get("latitude"),
                "longitude": gps.get("longitude"),
                "speed": gps.get("speedMilesPerHour", 0),
                "time": gps.get("time")
            })

        return trucks

    except Exception as e:
        return {"error": str(e)}


# ---------------- DOT FILE ----------------

@app.get("/api/eld/output-file", response_class=PlainTextResponse)
def eld_output_file():
    db = SessionLocal()

    eld_events = db.query(ELDEvent).all()
    edits = db.query(ELDEventEdit).all()
    certifications = db.query(DriverCertification).all()
    driver_logs = db.query(DriverLog).all()
    engine_events = db.query(EngineEvent).all()
    malfunctions = db.query(Malfunction).all()
    unassigned = db.query(UnassignedEvent).all()

    db.close()

    lines = []
    lines.append("ELD|USCITYLINK|0000000|" + now_utc())

    seq = 1

    lines.append("SECTION|DRIVER_LOGS")
    for log in driver_logs:
        lines.append(f"{seq}|DRIVER|{log.driver_id}|{log.action}|{log.timestamp}")
        seq += 1

    lines.append("SECTION|ENGINE_EVENTS")
    for eng in engine_events:
        lines.append(f"{seq}|ENGINE|{eng.vehicle_id}|{eng.status}|{eng.timestamp}")
        seq += 1

    lines.append("SECTION|ELD_EVENTS")
    for e in eld_events:
        event_code = get_event_code(e.duty_status)
        lines.append(
            f"{seq}|ELD|{e.id}|{e.driver_id}|{e.vehicle_id}|{event_code}|"
            f"{e.duty_status}|{e.event_time}|{e.latitude}|{e.longitude}|"
            f"{e.odometer}|{e.engine_hours}|{e.speed}|{e.event_origin}"
        )
        seq += 1

    lines.append("SECTION|UNASSIGNED_EVENTS")
    for u in unassigned:
        lines.append(
            f"{seq}|UNASSIGNED|{u.id}|{u.vehicle_id}|{u.event_time}|"
            f"{u.latitude}|{u.longitude}|{u.speed}|{u.assigned}|{u.assigned_driver}"
        )
        seq += 1

    lines.append("SECTION|EDIT_HISTORY")
    for edit in edits:
        lines.append(
            f"{seq}|EDIT|ORIGINAL:{edit.original_event_id}|NEW:{edit.new_event_id}|"
            f"{edit.driver_id}|{edit.old_status}|{edit.new_status}|"
            f"{edit.reason}|{edit.edited_at}"
        )
        seq += 1

    lines.append("SECTION|CERTIFICATIONS")
    for cert in certifications:
        lines.append(
            f"{seq}|CERTIFY|{cert.driver_id}|{cert.log_date}|"
            f"{cert.certified_at}|{cert.note}"
        )
        seq += 1

    lines.append("SECTION|MALFUNCTIONS")
    for m in malfunctions:
        lines.append(f"{seq}|MALFUNCTION|{m.type}|{m.description}|{m.timestamp}")
        seq += 1

    checksum = sum(len(line) for line in lines)
    lines.append(f"CHECKSUM|{checksum}")

    output = "\n".join(lines)

    with open("eld_output_test.txt", "w") as file:
        file.write(output)

    return output


@app.get("/api/eld/download")
def download_eld_file():
    eld_output_file()

    return FileResponse(
        "eld_output_test.txt",
        media_type="text/plain",
        filename="eld_output_test.txt"
    )
@app.post("/api/assignment/create")
def create_assignment(driver_id: str, vehicle_id: str):
    db = SessionLocal()

    old_assignments = (
        db.query(VehicleAssignment)
        .filter(VehicleAssignment.vehicle_id == vehicle_id)
        .filter(VehicleAssignment.active == "YES")
        .all()
    )

    for a in old_assignments:
        a.active = "NO"

    assignment = VehicleAssignment(
        driver_id=driver_id,
        vehicle_id=vehicle_id,
        assigned_at=now_utc(),
        active="YES"
    )

    db.add(assignment)
    db.commit()
    db.close()

    return {
        "message": "Driver assigned to vehicle",
        "driver_id": driver_id,
        "vehicle_id": vehicle_id
    }


@app.get("/api/assignment/active")
def get_active_assignments():
    db = SessionLocal()

    assignments = (
        db.query(VehicleAssignment)
        .filter(VehicleAssignment.active == "YES")
        .order_by(VehicleAssignment.id.desc())
        .all()
    )

    db.close()
    return assignments


@app.post("/api/assignment/create")
def create_assignment(driver_id: str, vehicle_id: str):
    db = SessionLocal()

    old_assignments = (
        db.query(VehicleAssignment)
        .filter(VehicleAssignment.vehicle_id == vehicle_id)
        .filter(VehicleAssignment.active == "YES")
        .all()
    )

    for a in old_assignments:
        a.active = "NO"

    assignment = VehicleAssignment(
        driver_id=driver_id,
        vehicle_id=vehicle_id,
        assigned_at=now_utc(),
        active="YES"
    )

    db.add(assignment)
    db.commit()
    db.close()

    return {
        "message": "Driver assigned to vehicle",
        "driver_id": driver_id,
        "vehicle_id": vehicle_id
    }


@app.get("/api/assignment/active")
def get_active_assignments():
    db = SessionLocal()

    assignments = (
        db.query(VehicleAssignment)
        .filter(VehicleAssignment.active == "YES")
        .order_by(VehicleAssignment.id.desc())
        .all()
    )

    db.close()
    return assignments