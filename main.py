import psycopg2
from pydantic import BaseModel
from datetime import datetime
from fastapi import FastAPI


class NotificationReceive(BaseModel):
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

    return {"status": "ok"}
