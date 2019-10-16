from gobcore.model import GOBModel
from gobcore.model.relations import get_relations, get_relation_name


def meta_info(catalog_name, collection_name, model):

    relations = [k for k in model['references'].keys()] + [k for k in model['very_many_references'].keys()]

    relation_names = [
        get_relation_name(GOBModel(), catalog_name, collection_name, relation)
        for relation in relations]

    meta_info = f"{' '.join(relation_names)}"
    return meta_info
