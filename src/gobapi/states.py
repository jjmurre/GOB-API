import datetime

from gobcore.model import GOBModel
from gobcore.typesystem import get_gob_type

from gobapi.storage import get_collection_states, _get_convert_for_state


END_OF_TIME = datetime.date(9999, 12, 31)


RELATED_COLLECTION_FIELDS = [
    'identificatie',
    'volgnummer',
    'code',
    'naam',
]


def _get_valid_state_in_timeslot(timeslot_start, timeslot_end, states):
    """Gets a valid state for a given timeslot from a list of states

    Returns a state.

    :param timeslot_start: The start date of the timeslot
    :param timeslot_end: The end date of the timeslot
    :param states: A list of states
    :return state: A valid state
    """
    for state in states:
        state_end = state.datum_einde_geldigheid if state.datum_einde_geldigheid \
                                                 else END_OF_TIME
        if state.datum_begin_geldigheid <= timeslot_start and state_end > timeslot_start:
            return state
    return None


def _get_valid_states_in_timeslot(timeslot_start, timeslot_end, collection_name,
                                  entity_id, relations, collections_with_state):
    """Gets all valid states for a given timeslot from a related collections
    The function is recursive to loop through all relations.

    Returns a list of valid states for each collection.

    :param timeslot_start: The start date of the timeslot
    :param timeslot_end: The end date of the timeslot
    :param collection_name: The name of the collection to search for
    :param entity_id: The current entity to search the valid state in
    :param relations: A dictionary for relations between the given collections
    :param collections_with_state: A dictionary with all states grouped by collection name
    :return valid states: A dict containing all valid entities grouped by timeslot
    """
    valid_states = {}

    # Get the states for the given entity id in the collection
    states = collections_with_state[collection_name][entity_id]
    valid_states[collection_name] = _get_valid_state_in_timeslot(timeslot_start, timeslot_end, states)

    # Try to get a related entity in another collection and try to find a valid state by calling this function again
    if collection_name in relations and valid_states[collection_name]:
        for field, relation in relations[collection_name].items():
            try:
                relation_entity_id = getattr(valid_states[collection_name], field)['id']
            except KeyError:
                # If no relation is found, skip checking for other relations
                pass
            else:
                valid_states.update(_get_valid_states_in_timeslot(timeslot_start=timeslot_start,
                                                                  timeslot_end=timeslot_end,
                                                                  collection_name=relation,
                                                                  entity_id=relation_entity_id,
                                                                  relations=relations,
                                                                  collections_with_state=collections_with_state))

    return valid_states


def _calculate_timeslots_for_entity(states, relations, collection_name,  # noqa: C901
                                   collections_with_state, timeslot_end=None):
    """Calculate all timeslots for an entity
    The function is recursive to loop through all relations.

    Returns a set of timeslots

    :param states: All states for the entity
    :param relations: A dictionary for relations between the given collections
    :param collection_name: The name of the collection to search for
    :param collections_with_state: A dictionary with all states grouped by collection name
    :param timeslot_end: An optional end date of the timeslot, empty only on the first pass to
        only add timeslots if they fit within the range of the main entity
    :return valid states: A dict containing all valid entities grouped by timeslot
    """
    timeslots = []
    for state in states:
        # If we are searching within a timeslot with an end, skip this state if it starts on or after the end
        if timeslot_end and state.datum_begin_geldigheid >= timeslot_end:
            continue
        timeslots.append(state.datum_begin_geldigheid)

        # Add the end date if it's set
        if state.datum_einde_geldigheid:
            timeslots.append(state.datum_einde_geldigheid)

        # If we have a relation, get all timeslots for that related entity, within the current timeslot
        if collection_name in relations:

            # Get the states timeslot end
            state_end = state.datum_einde_geldigheid if state.datum_einde_geldigheid \
                                                     else END_OF_TIME

            for field, relation in relations[collection_name].items():
                try:
                    relation_entity_id = getattr(state, field)['id']
                except KeyError:
                    pass
                else:
                    timeslots.extend(_calculate_timeslots_for_entity(
                                        states=collections_with_state[relation][relation_entity_id],
                                        relations=relations,
                                        collection_name=relation,
                                        collections_with_state=collections_with_state,
                                        timeslot_end=state_end))
    # Return unique sorted timeslots
    return sorted(set(timeslots))


def _find_relations(collections):
    """Find all relations between a list of collections

    Returns a dict of relation by collection and field name

    :param collections: A list of collections
    :return relations: A dict of relations
    """
    relations = {}
    for collection in collections:
        collection_name = f"{collection[0]}:{collection[1]}"
        relations[collection_name] = {}
        model_references = GOBModel().get_collection(collection[0], collection[1])['references']
        for field_name, reference in model_references.items():
            # Only include references for now
            if reference['type'] == 'GOB.Reference':
                relations[collection_name][field_name] = reference['ref']

    return relations


def _build_timeslot_rows(collections, entities_with_timeslots, primary_collection_name,    # noqa: C901
                         relations, collections_with_state, offset, limit):
    """Builds the output of the timeslot rows within the given offset and limit.
    A timeslot row is a state for an entity within a certain timeslot.

    Returns a list timeslot rows and the total count of timeslot rows

    :param collections: The collections for which states are returned
    :param entities_with_timeslots: A dict of timeslots grouped by entity id
    :param primary_collection_name: The name of the base collection we are exporting
    :param relations: A dictionary for relations between the given collections
    :param collections_with_state: A dictionary with all states grouped by collection name
    :return timeslot_rows, total_count: A list of timeslot rows and the total count of timeslot rows
    """
    row_count = 0
    timeslot_rows = []

    # for each timeslot get the valid state and related states
    for entity_id, timeslots in entities_with_timeslots.items():
        for count, timeslot in enumerate(timeslots):

            timeslot_start = timeslot
            timeslot_end = timeslots[count+1] if count+1 < len(timeslots) else END_OF_TIME

            valid_states = _get_valid_states_in_timeslot(timeslot_start=timeslot_start,
                                                         timeslot_end=timeslot_end,
                                                         collection_name=primary_collection_name,
                                                         entity_id=entity_id,
                                                         relations=relations,
                                                         collections_with_state=collections_with_state)

            # Only add a row if a valid state has been found for the primary collection
            if(valid_states[primary_collection_name]):
                row_count += 1
                # Don't store the row if it doesn't match the requested offset and limit
                if row_count <= offset or (row_count-offset) > limit:
                    continue

                # First fill the primary state
                catalog_name, collection_name = primary_collection_name.split(':')
                model = GOBModel().get_collection(catalog_name, collection_name)
                entity_convert = _get_convert_for_state(model)

                row = entity_convert(valid_states.pop(primary_collection_name))
                gob_date = get_gob_type("GOB.Date")

                row['begin_tijdvak'] = gob_date.from_value(timeslot_start)
                row['einde_tijdvak'] = gob_date.from_value(
                    timeslot_end if timeslot_end != END_OF_TIME else None)

                # Add the related states, so skip the first collection
                itercollections = iter(collections)
                next(itercollections)
                for collection in itercollections:
                    collection_name = ':'.join(collection)
                    model = GOBModel().get_collection(*collection)

                    entity_convert = _get_convert_for_state(model,
                                                            RELATED_COLLECTION_FIELDS)
                    try:
                        related_row = entity_convert(valid_states[collection_name])
                        related_row = {f'{collection_name}_{k}': v for k, v in related_row.items()}
                        row.update(related_row)
                    except KeyError:
                        # If a relation can't be found, add null values
                        related_row = {f'{collection_name}_{k}': None for k in RELATED_COLLECTION_FIELDS}
                        row.update(related_row)

                timeslot_rows.append(row)

    return timeslot_rows, row_count


def get_states(collections, offset, limit):
    """Get states for a list of collections

    Returns all timeslots and the related entities which are valid for each timeslot from the specified collections.

    :param collections: A list of lists containing catalog and collection
    :return states: A dict containing all valid entities grouped by cycle
    """
    # Get all relations for the specified collections
    relations = _find_relations(collections)

    primary_collection_name = f"{collections[0][0]}:{collections[0][1]}"
    collections_with_state = {
        f"{collection[0]}:{collection[1]}": get_collection_states(
            collection[0], collection[1]) for collection in collections
    }

    entities_with_timeslots = {}

    # Get the timeslots for each entity
    for entity_id, states in collections_with_state[primary_collection_name].items():
        unique_timeslots = _calculate_timeslots_for_entity(states=states,
                                                           relations=relations,
                                                           collection_name=primary_collection_name,
                                                           collections_with_state=collections_with_state)
        # Save the unique timeslots for each entity
        entities_with_timeslots[entity_id] = unique_timeslots

    timeslot_rows, total_count = _build_timeslot_rows(collections=collections,
                                                      entities_with_timeslots=entities_with_timeslots,
                                                      primary_collection_name=primary_collection_name,
                                                      relations=relations,
                                                      collections_with_state=collections_with_state,
                                                      offset=offset,
                                                      limit=limit)
    return timeslot_rows, total_count
