# Pumpwood Flask Views
Assist creation of Pumpwood views in Flask.

<a href="https://github.com/Murabei-OpenSource-Codes/pumpwood-miscellaneous">
  pumpwood-flaskviews
</a>.

<p align="center" width="60%">
  <img src="doc/sitelogo-horizontal.png" /> <br>

  <a href="https://en.wikipedia.org/wiki/Cecropia">
    Pumpwood is a native brasilian tree
  </a> which has a symbiotic relation with ants (Murabei)
</p>

# Description
This package assist the creation of views in flask using Pumpwood pattern.

## pumpwood_flaskviews.action
Expose models functions at the API. It is possible to expose normal and
classmethods, the first argument for each one should respect the convention
self and cls respectively.

```
from pumpwoodflask_views.action import action


class Person(db.Model):
    """Citizen of a far far way country."""

    name = db.Column(db.String(3), nullable=True, doc=(
        "csv field delimiter when using file input"))
    birth_date = db.DateTime(db.String(3), nullable=True, doc=(
        "csv decimal delimiter when using file input"))

    __tablename__ = 'person'

    @action(info='Marry person to another.')
    def marry(self, person_id: int, contract: str = None) -> bool:
        """
        Marry person to another one.

        Args
        ----
        person_id: int
          Id of the person to whom the object should be married.

        Kwargs
        ----------
        contract: str
          Set the legal contract used.

        Returns
        -------
        bool
            Returns true if it was possible to process the action.
        """
        ...
        return True

    @classmethod
    @action(info='Send cards to today's birthday.')
    def process_birthday_cards(cls, reference_date: datetime.datetime) -> int:
        """
        Send birthday cards to every one with birthday today. If reference_date
        if reference_date is passed it will be used instead of today as
        reference.

        Args
        ----
        No args.

        Kwargs
        ----------
        reference_date: datetime.datetime
          Reference date to check for birthdays.

        Returns
        -------
        int
            Number of birthday cards sent.
        """
        ...
        return True
```

```
from pumpwood_communication.microservices import PumpWoodMicroService

microservice = PumpWoodMicroService(
    server_url="http://0.0.0.0:8080/",
    username="pumpwood", password="pumpwood")
microservice.login()

# List the actions avaiable for the model Person, it will include information
# about the parameters and also the doc string associated with the function.
person_actions = microservice.list_actions(model_class="Person")

# Return the serialized person object, the parameters used at the action and
# the result
results = microservice.excute_action(
  model_class="Person", pk=3, action="marry",
  parameters={"person_id": 1})

# process_birthday_cards is a classfunction so it is not associated with an
# object
microservice.excute_action(
  model_class="Person", action="process_birthday_cards")
```

## pumpwood_flaskviews.auth
Integrate pumpwood authentication with flask end-points. After setting the
Auth host it is possible o call check_authorization, it will pass request
headers to auth end-point and check if user is authenticated.

```
AuthFactory.set_server_url(server_url=config_dict['AUTH_SERVER'])

def flask_end_point():
  AuthFactory.check_authorization()
  return ...
```

## pumpwood_flaskviews.fields
Extend SQLAlchemy fields for some common used fields in Pumpwood.
- GeometryField: Serialize shapely geometry fields.
- ChoiceField: Serialize sqlalchemy_utils ChoiceType fields.


## pumpwood_flaskviews.serializers
Define a base serializer for pumpwood models which always return at least pk
and model_class.

## pumpwood_flaskviews.views
Define pumpwood basic views. They have always the same pattern:

<b>PumpWoodFlaskView</b>
- list (/rest/[model_class]/list/): List objects using query parameters
    passed as dictionary payload, paginate by 50.
- list_without_pag (/rest/[model_class]/list-without-pag/): Same as list,
    but return all objects.
- list_one (/rest/[model_class]/list-one/): List one object using list
    serialize (fewer fields).
- retrieve (/rest/[model_class]/retrieve/[pk]): Return all information from
    one object.
- object_template (/rest/[model_class]/retrieve/): Return an empty object
  template.
- retrieve_file (/rest/[model_class]/retrieve-file/[pk]?file-field=[field]):
    retrieve file which path is saved on file-field argument.
- remove_file_field (/rest/[model_class]/remove-file-field/[pk]?file-field=[field]):
    delete file  which path is saved on file-field argument using streaming.
- retrieve_file_streaming (/rest/[model_class]/retrieve-file-streaming/[pk]?file-field=[field]):
  retrieve file which path is saved on file-field argument using streaming.
- save_file_streaming (/rest/[model_class]/save-file-streaming/[pk]?file-field=[field]):
  Save a file in file-field using streaming.
- save (/rest/[model_class]/save/) Save file object passed in payload.
- delete (/rest/[model_class]/delete/[pk]): Remove an object from database.
- delete_many (/rest/[model_class]/delete/): Delete many objects using
  query dictionary.
- list_actions (/rest/[model_class]/actions/): List actions available for
  model_class.
- execute_action (/rest/[model_class]/actions/[action name]/[pk]): Run action
    over object pk.
- search_options (/rest/[model_class]/options/): Retrieve information for
    list fields and filters.
- fill_options (/rest/[model_class]/options/): Pass an incomplete object as
    post payload and receives the validation of the fields and update the
    choice possibilities.

<b>PumpWoodDataFlaskView</b>
- Same as PumpWoodFlaskView...
- pivot (/rest/[model_class]/pivot/): Retrieve data using query dict, but
    instead of using serializers use pandas data frame and parse result with
    to_dict. It is possible to pivot data using the columns.
- bulk_save (/rest/[model_class]/bulk-save/): Bulck save data on database.

<b>PumpWoodDimentionsFlaskView</b>
- Same as PumpWoodFlaskView...
- list_dimentions (/rest/[model_class]/list-dimentions/): List avaiable
    dimentions in objects resulting from the query.
- list_dimention_values (/rest/[model_class]/list-dimention-values/): List
    values associated with dimentions in objects resulting from the query.

Example for defining a PumpWoodFlaskView:
```
from pumpwoodflask_views.views import PumpWoodFlaskView
from models import Person
from serializers import PersonSerializer
from singletons import storage_object, microservice


class PersonView(PumpWoodFlaskView):
    description = "Person"

    # Used when registering end-point on routes
    dimentions = {
        "service": "test-service",
        "type": "human",
    }
    icon = None

    db = db
    model_class = Person
    storage_object = storage_object
    microservice = microservice
    serializer = PersonSerializer
    list_fields = [
        'pk', 'model_class', 'name', 'birthday', 'married_to_id']
    foreign_keys = {
        'married_to_id': {'model_class': 'Person', 'many': False}
    }
```
