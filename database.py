import datetime 
import os
import json
from abc import ABCMeta, abstractmethod
from typing import Union, Optional, TypedDict, List

# TODO: Configparser integration

class Database(metaclass=ABCMeta):
    """
    Creates a blueprint for all the database types and subclasses to follow.

    Extends ABCMeta to make the class abstract and uninstantiable.
    Class attribute __instance ensures there is only one instance of the class.

    .. seealso:: :class: `LocalDatabase` 
    
    """
    __instance = None

    def __init__(self):
        """
        Initializes the class attributes.

        self.last_accessed keeps track of the last time the database was accessed (:func: `get_data`).
        self.last_modified keeps track of the last time the database was modified (:func: `set_data`, :func: `delete_data`).
        self.created_at keeps track of the time the database was created.
        """
        self.last_accessed = None
        self.last_modified = None
        self.created_at = datetime.datetime.now()

    @abstractmethod
    def get_data(self, key: str, *subkeys) -> Union[dict, str]:
        ...

    @abstractmethod
    def set_data(self, key: str, value, *subkeys) -> bool: 
        ...

    @abstractmethod
    def delete_data(self, key: str, *subkeys) -> bool:
        ... 

class DeserializedKeysDict(TypedDict):
    """
    A type hint for the deserialized keys dictionary.

    Class attribute main_key is the main key of the dictionary (the first key passed).
    Class attribute subkeys is a list of subkeys (the rest of the keys passed).

    .. seealso:: :class: `LocalDatabase`
    """
    main_key: str
    subkeys: Optional[List[str]]

class KeyNotFoundError(Exception):
    """
    An exception that is raised when the key is not found in the database.

    Extends Exception to make the class an exception.

    .. seealso:: :class: `LocalDatabase`
    """

    def __init__(self, key):
        """
        Initializes the class attributes.

        Extend Exception's __init__ method to set the message of the exception.

        self.key is the key that was not found in the database.
        """

        self.key = key
        super().__init__(
            f'Key not found in DB records [{self.key}]')
        
    def __str__(self): 
        return f'Key not found in DB records [{self.key}]'
    
    def __repr__(self):
        return f'Key not found in DB records [{self.key}]'
    
    def __reduce__(self):
        return (self.__class__, (self.key,))

class LocalDatabase(Database):
    """
    A class that represents a local database.

    Extends Database to inherit the abstract methods.

    Class attribute __instance ensures there is only one instance of the class.
    
    .. seealso:: :class: `Database`
    """

    __instance = None

    def __new__(cls, path: str, *args, **kwargs):
        """
        Creates a new instance of the class if there is no instance already.

        If there is an instance, it returns the instance instead of creating a new one.

        :param cls: The class that is being instantiated.
        :type cls: class
        :param path: The path to the database file.
        :type path: str
        :return: The instance of the class.
        :rtype: :class: `LocalDatabase`
        :raises: FileNotFoundError: If the database file is not found. (or not a JSON file)

        .. seealso:: :class: `Database`
        .. note:: If you want to create multiple instances of the class, you can safely remove this function.
        """
        if cls.__instance is None:
            if not os.path.exists(path) or not os.path.isfile(path) or not path.endswith('.json'):
                raise FileNotFoundError(f'Could not find the database file at {path} [The file must be a JSON file]')
            
            cls.__instance = super().__new__(cls, *args, **kwargs)
            print(f'A new database connection has been created successfully! [ID: {id(cls.__instance)}]')
        else: 
            print(f'A database connection has already been made. Reusing the same connection. [ID: {id(cls.__instance)}]')
        return cls.__instance

    def __init__(self, path: str):
        """
        Initializes the class attributes.

        Extends Database's __init__ method to use its attributes.

        self.path is the path to the database file.

        :param path: The path to the database file.
        :type path: str
        
        .. seealso:: :class: `Database`
        """
        print('Connected to the database!')
        self.path = path
        super().__init__()

    def __str__(self):
        """Returns a string representation of the class. (User friendly)"""
        return f'LocalDatabase Connection (ID: {id(self)})'
    
    def __repr__(self):
        """Returns a string representation of the class. (Developer friendly)"""
        return f'Database Connection (ID: {id(self)})'
    
    def __del__(self):
        """Disconnects from the database when the instance is deleted."""
        print('Disconnected from the database!')
        self.__instance = None

    def get_data(self, key: str) -> Union[dict, str]:
        """
        Gets the data from the database.

        :param key: The key to get the data from.
        :type key: str
        :return: The data from the database.
        :rtype: Union[dict, str]

        .. seealso:: :class: `Database`
        """
        with open(self.path, 'rt', encoding='utf-8') as db_file:
            db = json.load(db_file)
            self.last_accessed = datetime.datetime.now()

            key, subkeys = self.__deserialize_key(key).values()

            data: Union[dict, str] = db.get(key, None)

            if subkeys and len(subkeys) > 0 and isinstance(data, dict):
        
                for subkey in subkeys:
                    data = data.get(subkey, None)
                    if data is None: 
                        break # TODO: hata fırlat
                    
            return data
        
    def set_data(self, key: str, value: Union[dict, str]) -> bool:
        """
        Sets the data to the database.

        :param key: The key to set the data to.
        :type key: str
        :param value: The value to set to the key.
        :type value: Union[dict, str]
        :return: True if the data is set successfully, False otherwise.
        :rtype: bool
        :raises: KeyNotFoundError: If the key is not found in the database.

        .. seealso:: :class: `Database`
        """
        with open(self.path, 'r+t', encoding='utf-8') as db_file:
            db = json.load(db_file)
            self.last_modified = datetime.datetime.now()

            key, subkeys = self.__deserialize_key(key).values()

            if not key in db: 
                raise KeyNotFoundError(key)

            if subkeys and len(subkeys) > 0:
                self.__find_nested_data(db_content=db, key=key, operation='set', value=value, subkeys=subkeys)
            else: 
                db[key] = value 
        
            return self.__update_database(db, db_file)

    def delete_data(self, key: str) -> bool:
        """
        Deletes the data from the database.

        :param key: The key to delete the data from.
        :type key: str
        :return: True if the data is deleted successfully, False otherwise.
        :rtype: bool

        .. seealso:: :class: `Database`
        """
        with open(self.path, 'r+t', encoding='utf-8') as db_file:
            self.last_modified = datetime.datetime.now()
            db = json.load(db_file)

            key, subkeys = self.__deserialize_key(key).values()

            if subkeys and len(subkeys) > 0:
                self.__find_nested_data(db_content=db, key=key, operation='delete', subkeys=subkeys)
            else:
                db.pop(key, None)

            return self.__update_database(db, db_file)

    def __update_database(self, new_db_content: dict, db_file):
        """
        Updates the database file with the new content.

        :param new_db_content: The new content to update the database file with.
        :type new_db_content: dict
        :param db_file: The database file to update.
        :type db_file: file
        :return: True if the database is updated successfully, False otherwise.
        :rtype: bool

        .. seealso:: :class: `Database`
        """
        db_file.seek(0)
        json.dump(new_db_content, db_file, indent=4, ensure_ascii=False)
        db_file.truncate()
        return True

    def __find_nested_data(self, *, db_content: dict, key: str, subkeys: List[str], operation: str, value: Optional[Union[dict, str]] = '') -> Union[dict, str]:
        """
        Finds the nested data in the database.

        :param db_content: The database content to find the nested data in.
        :type db_content: dict
        :param key: The key to find the nested data in.
        :type key: str
        :param subkeys: The subkeys to find the nested data in.
        :type subkeys: List[str]
        :param operation: The operation to do with the nested data.
        :type operation: str
        :param value: The value to set to the nested data.
        :type value: Optional[Union[dict, str]]
        :return: The nested data.
        :rtype: Union[dict, str]

        .. seealso:: :class: `Database`
        """
        if subkeys and len(subkeys) > 0:
            current_dict = db_content[key]
            for key in subkeys[:-1]:
                current_dict = current_dict.setdefault(key, {})
            if operation == 'delete':
                current_dict.pop(subkeys[-1], None)
            elif operation == 'set':
                current_dict[subkeys[-1]] = value
        else:
            return db_content[key]

    def __deserialize_key(self, key: List[str]) -> DeserializedKeysDict:
        """
        Deserializes the key.

        :param key: The key to deserialize.
        :type key: List[str]
        :return: The deserialized key.
        :rtype: DeserializedKeysDict

        .. seealso:: :class: `Database`
        """
        # 'users/user1' -> ['users', 'user1']
        key = key.strip().split('/')
        return {
            'main_key': key[0],
            'subkeys': key[1:]
        }