import psycopg2
from datetime import datetime


class Notification:
    def __init__(self, id: int, user_id: str, message: str, send_at: str):
        self.dct = {'id': id, 'user_id': user_id, 'message': message,
                    'send_at': datetime.strptime(send_at, "%Y-%m-%dT%H:%M:%S")}
# оставил словарь, т.к. было такое задание в прошлой задаче. Здесь, при работе с SQL, мне он уже кажется лишним. Ключ Priority убрал, т.к. его нет в таблице

    def __repr__(self):
        return f'{self.dct.get('user_id')}: {self.dct.get('message')}'

    def send_at(self):
        return self.dct['send_at']


class Scheduler:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname="notifications",
            user="admin",
            password="verystrongpassword",
            host="localhost"
        )
        self.cursor = self.conn.cursor()

    def schedule(self, notification):
        self.cursor.execute(
            """
            INSERT INTO scheduled_notifications (user_id, message, send_at, status)
            VALUES (%s, %s, %s, 'pending')
            """,
            (notification.dct['user_id'], notification.dct['message'], notification.dct['send_at'])
        )
        self.conn.commit()

    def run_pending(self):
        with self.conn, self.conn.cursor() as c:
            c.execute(
                """
                SELECT id, user_id, message, send_at
                FROM scheduled_notifications
                WHERE status = 'pending' AND send_at <= %s       
                """,
                (datetime.now(),)
            )
            pending = c.fetchall()
            for elem in pending:
                notification = Notification(*elem)
                send_notification(notification)
                c.execute(
                    """
                    UPDATE scheduled_notifications
                    SET status = 'sent'
                    WHERE id = %s
                    """,
                    (elem[0],)
                )


def send_notification(notification: Notification):
    print(f"Sending: {notification}")
