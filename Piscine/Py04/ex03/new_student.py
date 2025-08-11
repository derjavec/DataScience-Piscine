import random
import string
from dataclasses import dataclass, field


def generate_id() -> str:
    return "".join(random.choices(string.ascii_lowercase, k=15))


@dataclass
class Student:
    nam: str
    surname: str
    active: bool = field(init=False)
    login: str = field(init=False)
    id: str = field(init=False)

    def __post_init__(self):
        self.active = True
        self.login = self.name[0].upper() + self.surname.capitalize()
        self.id = generate_id()
