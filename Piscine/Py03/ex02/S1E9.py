from abc import ABC, abstractmethod


class Character(ABC):
    """Abstract class representing a character with a name and alive status."""

    def __init__(self, first_name: str, is_alive: bool = True):
        """
        Initialize a character.
        """
        self.first_name = first_name
        self.is_alive = is_alive

    @abstractmethod
    def die(self):
        """
        Marks the character as dead by setting is_alive to False.
        Abstract method that must be implemented by subclasses.
        """
        pass


class Stark(Character):
    """Class representing a Stark family character."""

    def __init__(self, first_name: str, is_alive: bool = True):
        """
        Initialize a Stark character.
        """
        super().__init__(first_name, is_alive)

    def die(self):
        """Sets the character's alive status to False, marking them as dead."""
        self.is_alive = False
