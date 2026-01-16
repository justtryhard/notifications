import psycopg2
from pydantic import BaseModel
from datetime import datetime
from fastapi import FastAPI, HTTPException


class NotificationReceive(BaseModel):  # модель данных для валидации
    user_id: str
    message: str
    send_at: datetime


class Scheduler:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname="notifications",
            user="postgres",
            password="verystrong",
            host="localhost",
            sslmode="disable"
        )
        self.cursor = self.conn.cursor()

    def schedule(self, user_id: str, message: str, send_at):
        self.cursor.execute(
            """
            INSERT INTO scheduled_notifications (user_id, message, send_at, status)
            VALUES (%s, %s, %s, 'pending')
            """,
            (user_id, message, send_at)
        )
        self.conn.commit()


app = FastAPI()
scheduler = Scheduler()


@app.post("/notifications")
def create_notification(notification: NotificationReceive):
    scheduler.schedule(
        user_id=notification.user_id,
        message=notification.message,
        send_at=notification.send_at
    )

    return {"user_id": notification.user_id, "message": notification.message, "status": "scheduled"}


@app.get("/notifications")
def get_notifications_from_user(user_id: str = None):
    if user_id:
        scheduler.cursor.execute(
            "SELECT id, user_id, message, send_at, status "
            "FROM scheduled_notifications WHERE user_id = %s;",
            (user_id,)
        )
    else:
        scheduler.cursor.execute(
                "SELECT id, user_id, message, send_at, status FROM scheduled_notifications"
            )
    lines = scheduler.cursor.fetchall()
    list_of_lines = [
        {"id": line[0],
         "user_id": line[1],
         "message": line[2],
         "send_at": line[3],
         "status": line[4]}
        for line in lines]
    return list_of_lines


@app.delete("/notifications/{notification_id}")
def remove_notification(notification_id: int):
    scheduler.cursor.execute(
        "DELETE FROM scheduled_notifications WHERE id = %s RETURNING id",
        (notification_id,)
    )
    line = scheduler.cursor.fetchone()
    scheduler.conn.commit()

    if not line:
        raise HTTPException(status_code=404, detail="Notification not found")

    return {"id": notification_id, "status": "deleted"}


@app.patch("/notifications/{notification_id}")
def update_notification(notification_id: int, msg: str):
    scheduler.cursor.execute(
        """
        UPDATE scheduled_notifications
        SET message = %s
        WHERE id = %s
        RETURNING id
        """,
        (msg, notification_id)
    )
    line = scheduler.cursor.fetchone()
    scheduler.conn.commit()

    if not line:
        raise HTTPException(status_code=404, detail="Notification not found")

    return {"id": notification_id, "message": msg}
