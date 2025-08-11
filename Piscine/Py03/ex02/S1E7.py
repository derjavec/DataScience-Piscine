from S1E9 import Character


class Baratheon(Character):
    """Representing the Baratheon family."""
    def __init__(self, first_name: str, is_alive: bool = True):
        super().__init__(first_name, is_alive)
        self.family_name = "Baratheon"
        self.eyes = "Brown"
        self.hairs = "Dark"

    def die(self):
        """Sets the character's alive status to False, marking them as dead."""
        self.is_alive = False

    def __str__(self):
        return
        f"Baratheon('{self.first_name}', eyes={self.eyes}, hairs={self.hairs})"

    def __repr__(self):
        return
        f"Baratheon('{self.first_name}', \
        eyes={self.eyes}, hairs={self.hairs})"


class Lannister(Character):
    """Representing the Lannister family."""
    def __init__(self, first_name: str, is_alive: bool = True):
        super().__init__(first_name, is_alive)
        self.family_name = "Lannister"
        self.eyes = "Blue"
        self.hairs = "Light"

    def die(self):
        """Sets the character's alive status to False, marking them as dead."""
        self.is_alive = False

    def __str__(self):
        return
        f"Lannister('{self.first_name}', eyes={self.eyes}, hairs={self.hairs})"

    def __repr__(self):
        return
        f"Lannister('{self.first_name}', eyes={self.eyes}, hairs={self.hairs})"

    @classmethod
    def create_lannister(cls, first_name: str, is_alive: bool = True):
        """
        Class method to create a Lannister character chain.
        """
        return cls(first_name, is_alive)
