from datetime import datetime

### test1111dasdasfasf11
class Notification:
    def __init__(self, id: int, user_id: str, message: str, send_at: str, priority: str):
        self._dct = {'id': id, 'user_id': user_id, 'message': message,
                     'send_at': datetime.strptime(send_at, "%Y-%m-%dT%H:%M:%S"), 'priority': priority}

    def __repr__(self):
        return f'{self._dct.get('user_id')}: {self._dct.get('message')}'

    def send_at(self):
        return self._dct.get('send_at')


class Scheduler:
    def __init__(self):
        self._notifications = []

    def schedule(self, notification):
        self._notifications.append(notification)

    def run_pending(self):
        ready = []
        pending = []
        for elem in self._notifications:
            if elem.send_at() < datetime.now():
                ready.append(elem)
            else:
                pending.append(elem)
        for n in ready:
            send_notification(n)

        self._notifications[:] = pending


def send_notification(notification: Notification):
    print(f"Sending: {notification}")