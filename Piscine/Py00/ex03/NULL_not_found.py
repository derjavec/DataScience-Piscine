def NULL_not_found(object: any) -> int:

    if object is None:
        print(f"Nothing : {type(object)}")

    elif isinstance(object, float) and object != object:
        print(f"Cheese: {type(object)}")

    elif isinstance(object, int) and object == 0:
        print(f"Zero : {type(object)}")

    elif isinstance(object, str) and object == '':
        print(f"Empty : {type(object)}")

    elif isinstance(object, bool) and object is False:
        print(f"Fake : {type(object)}")

    else:
        print("Type not found")
        return 1

    return 0
