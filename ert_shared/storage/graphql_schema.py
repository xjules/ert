import graphene
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker, Query as ORMQuery
from graphene_sqlalchemy import SQLAlchemyObjectType, SQLAlchemyConnectionField

from ert_shared.storage import connections
from ert_shared.storage.blob_api import BlobApi
from ert_shared.storage.model import Project as ProjectModel, Ensemble as EnsembleModel, \
    Realization as RealizationModel, Response as ResponseModel, ResponseDefinition as ResponseDefinitionModel, Entities, \
    Observation as ObservationModel, ParameterPrior as ParameterPriorModel, \
    ObservationResponseDefinitionLink as ObsResDefLinkModel, Update as UpdateModel, \
    ParameterDefinition as ParameterDefinitionModel, Parameter as ParameterModel

engine = create_engine('sqlite:///entities.db', convert_unicode=True)

db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))

Entities.query = db_session.query_property()


def fetch_blob_from_ref(ref):
    _blob_connection = connections.get_blob_connection('sqlite:///blobs.db')
    blob_api = BlobApi(connection=_blob_connection)
    blob = blob_api.get_blob(ref)
    return blob.data


class Observation(SQLAlchemyObjectType):
    values = graphene.List(graphene.Float)
    stds = graphene.List(graphene.Float)
    key_indexes = graphene.List(graphene.Int)
    data_indexes = graphene.List(graphene.Int)

    class Meta:
        model = ObservationModel
        exclude_fields = ('values_ref', 'stds_ref', 'key_indexes_ref', 'data_indexes_ref')

    def resolve_values(self, info):
        return fetch_blob_from_ref(self.values_ref)

    def resolve_stds(self, info):
        return fetch_blob_from_ref(self.stds_ref)

    def resolve_key_indexes(self, info):
        return fetch_blob_from_ref(self.key_indexes_ref)

    def resolve_data_indexes(self, info):
        return fetch_blob_from_ref(self.data_indexes_ref)


class Realization(SQLAlchemyObjectType):
    class Meta:
        model = RealizationModel


class Ensemble(SQLAlchemyObjectType):
    update_source = graphene.String()

    class Meta:
        model = EnsembleModel

    def resolve_update_source(self, info):
        if self.parent is not None:
            return self.parent.ensemble_reference.name
        return None



class Response(SQLAlchemyObjectType):
    values = graphene.List(graphene.Float)
    name = graphene.String()

    class Meta:
        model = ResponseModel
        exclude_fields = ('values_ref',)

    def resolve_values(self, info):
        return fetch_blob_from_ref(self.values_ref)

    def resolve_name(self, info):
        pd = ResponseDefinition.get_query(info).get(self.response_definition_id)
        return pd.name



class ResponseDefinition(SQLAlchemyObjectType):
    indexes = graphene.List(graphene.Int)
    observations = graphene.List(of_type=Observation)

    class Meta:
        model = ResponseDefinitionModel
        exclude_fields = ('indexes_ref', 'observation_links')

    def resolve_indexes(self, info):
        return fetch_blob_from_ref(self.indexes_ref)

    def resolve_observations(self, info):
        links = _ObsResDefLink.get_query(info).filter_by(response_definition_id=self.id)

        result = []
        for link in links:
            obs = Observation.get_query(info).get(link.observation_id)
            result.append(obs)
        return result


class ParameterPrior(SQLAlchemyObjectType):
    class Meta:
        model = ParameterPriorModel


class Update(SQLAlchemyObjectType):
    class Meta:
        model = UpdateModel


class ParameterDefinition(SQLAlchemyObjectType):
    class Meta:
        model = ParameterDefinitionModel


class Parameter(SQLAlchemyObjectType):
    value = graphene.Float()
    name = graphene.String()

    class Meta:
        model = ParameterModel
        exclude_fields = ('value_ref',)

    def resolve_value(self, info):
        return fetch_blob_from_ref(self.value_ref)

    def resolve_name(self, info):
        pd = ParameterDefinition.get_query(info).get(self.parameter_definition_id)
        return pd.name


class _ObsResDefLink(SQLAlchemyObjectType):
    class Meta:
        model = ObsResDefLinkModel


class Query(graphene.ObjectType):
    ensemble = graphene.Field(Ensemble, id=graphene.Int(required=True))
    all_ensembles = graphene.List(Ensemble)

    def resolve_ensemble(self, info, id=None):
        return Ensemble.get_query(info).filter_by(id=id).first()

    def resolve_all_ensembles(self, info):
        return Ensemble.get_query(info).all()


schema = graphene.Schema(query=Query, types=[Ensemble, Realization, ResponseDefinition, Response, Observation])
