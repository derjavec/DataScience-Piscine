from S1E7 import Baratheon, Lannister


class King(Baratheon, Lannister):
    """
    King class inheriting from Baratheon and Lannister families.
    Allows getting and setting physical traits like eyes and hairs.
    """
    def __init__(self, first_name: str, is_alive: bool = True):
        super().__init__(first_name, is_alive)

    def set_eyes(self, value):
        self.eyes = value

    def get_eyes(self):
        return self.eyes

    def set_hairs(self, value):
        self.hairs = value

    def get_hairs(self):
        return self.hairs
