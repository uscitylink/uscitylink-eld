from fastapi import FastAPI
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import requests
import os
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = "sqlite:///./eld.db"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# =========================
# MODELS
# =========================

class GPSPoint(Base):
    __tablename__ = "gps_points"

    id = Column(Integer, primary_key=True, index=True)
    vehicle_id = Column(String)
    samsara_vehicle_id = Column(String)
    latitude = Column(String)
    longitude = Column(String)
    speed = Column(Float, default=0)
    gps_time = Column(String)
    saved_at = Column(String)

class VehicleAssignment(Base):
    __tablename__ = "vehicle_assignments"

    id = Column(Integer, primary_key=True, index=True)
    driver_id = Column(String)
    vehicle_id = Column(String)
    assigned_at = Column(String)
    active = Column(String, default="YES")

Base.metadata.create_all(bind=engine)

# =========================
# HELPERS
# =========================

def now_utc():
    return datetime.utcnow().isoformat()

def get_token():
    token = os.environ.get("SAMSARA_API_TOKEN")
    if token:
        token = token.strip().replace("Bearer ", "")
    return token

# =========================
# SAMSARA GPS (LIVE)
# =========================

@app.get("/api/samsara/gps")
def get_samsara_gps():
    token = get_token()

    if not token:
        return {"error": "No token"}

    url = "https://api.samsara.com/fleet/vehicles/stats"

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    params = {
        "types": "gps"
    }

    res = requests.get(url, headers=headers, params=params)

    if res.status_code != 200:
        return {
            "error": "Samsara API error",
            "status_code": res.status_code,
            "response": res.text
        }

    data = res.json()

    trucks = []

    for v in data.get("data", []):
        gps = v.get("gps")
        if not gps:
            continue

        trucks.append({
            "vehicle_id": v.get("name") or v.get("id"),
            "samsara_vehicle_id": v.get("id"),
            "latitude": gps.get("latitude"),
            "longitude": gps.get("longitude"),
            "speed": gps.get("speedMilesPerHour", 0),
            "time": gps.get("time")
        })

    return trucks

# =========================
# SAVE GPS TO DATABASE
# =========================

@app.post("/api/samsara/save-gps")
def save_gps():
    data = get_samsara_gps()

    if not isinstance(data, list):
        return data

    db = SessionLocal()
    saved = 0

    for t in data:
        existing = db.query(GPSPoint).filter(
            GPSPoint.vehicle_id == t["vehicle_id"],
            GPSPoint.gps_time == t["time"]
        ).first()

        if existing:
            continue

        point = GPSPoint(
            vehicle_id=t["vehicle_id"],
            samsara_vehicle_id=t["samsara_vehicle_id"],
            latitude=str(t["latitude"]),
            longitude=str(t["longitude"]),
            speed=float(t["speed"]),
            gps_time=t["time"],
            saved_at=now_utc()
        )

        db.add(point)
        saved += 1

    db.commit()
    db.close()

    return {"saved": saved}

# =========================
# ROUTE FROM DATABASE
# =========================

@app.get("/api/gps/route")
def get_route(vehicle_id: str):
    db = SessionLocal()

    rows = db.query(GPSPoint)\
        .filter(GPSPoint.vehicle_id == vehicle_id)\
        .order_by(GPSPoint.gps_time.asc())\
        .all()

    db.close()

    route = []

    for r in rows:
        route.append({
            "vehicle_id": r.vehicle_id,
            "latitude": r.latitude,
            "longitude": r.longitude,
            "speed": r.speed,
            "time": r.gps_time
        })

    return route

# =========================
# ASSIGNMENT APIs
# =========================

@app.post("/api/assignment/create")
def create_assignment(driver_id: str, vehicle_id: str):
    db = SessionLocal()

    db.query(VehicleAssignment).filter(
        VehicleAssignment.driver_id == driver_id
    ).update({"active": "NO"})

    db.query(VehicleAssignment).filter(
        VehicleAssignment.vehicle_id == vehicle_id
    ).update({"active": "NO"})

    new = VehicleAssignment(
        driver_id=driver_id,
        vehicle_id=vehicle_id,
        assigned_at=now_utc(),
        active="YES"
    )

    db.add(new)
    db.commit()
    db.close()

    return {"message": "saved"}

@app.get("/api/assignment/active")
def active_assignments():
    db = SessionLocal()

    rows = db.query(VehicleAssignment)\
        .filter(VehicleAssignment.active == "YES")\
        .all()

    db.close()

    return [
        {
            "driver_id": r.driver_id,
            "vehicle_id": r.vehicle_id,
            "assigned_at": r.assigned_at,
            "active": r.active
        }
        for r in rows
    ]