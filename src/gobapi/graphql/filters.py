"""Graphene filters

"""
from graphene_sqlalchemy import SQLAlchemyConnectionField


class FilterConnectionField(SQLAlchemyConnectionField):
    RELAY_ARGS = ['first', 'last', 'before', 'after', 'sort']

    @classmethod
    def get_query(cls, model, info, **args):
        query = super(FilterConnectionField, cls).get_query(model, info, **args)
        for field, value in args.items():
            if field not in cls.RELAY_ARGS:
                if value == "null":
                    query = query.filter(getattr(model, field) == None)
                else:
                    query = query.filter(getattr(model, field) == value)
        return query
