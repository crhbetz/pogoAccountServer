def can_be_type(obj, Type):
    try:
        Type(obj)
        return True
    except Exception:
        return False

