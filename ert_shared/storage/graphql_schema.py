import graphene
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker, Query as ORMQuery
from graphene_sqlalchemy import SQLAlchemyObjectType, SQLAlchemyConnectionField
import pandas as pd

from ert_shared.storage import connections
from ert_shared.storage.blob_api import BlobApi
from ert_shared.storage.storage_api import StorageApi
from ert_shared.storage.model import (
    Project as ProjectModel,
    Ensemble as EnsembleModel,
    Realization as RealizationModel,
    Response as ResponseModel,
    ResponseDefinition as ResponseDefinitionModel,
    Entities,
    Observation as ObservationModel,
    ParameterPrior as ParameterPriorModel,
    ObservationResponseDefinitionLink as ObsResDefLinkModel,
    Update as UpdateModel,
    ParameterDefinition as ParameterDefinitionModel,
    Parameter as ParameterModel,
)

rdb_url = "sqlite:///entities.db"
blob_url = "sqlite:///blobs.db"

engine = create_engine(rdb_url, convert_unicode=True)

db_session = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine)
)

Entities.query = db_session.query_property()


data_blob = {}


def fetch_blob_from_ref(ref):
    _blob_connection = connections.get_blob_connection(blob_url)
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
        exclude_fields = (
            "values_ref",
            "stds_ref",
            "key_indexes_ref",
            "data_indexes_ref",
        )

    def resolve_values(self, info):
        return fetch_blob_from_ref(self.values_ref)

    def resolve_stds(self, info):
        return fetch_blob_from_ref(self.stds_ref)

    def resolve_key_indexes(self, info):
        return fetch_blob_from_ref(self.key_indexes_ref)

    def resolve_data_indexes(self, info):
        return fetch_blob_from_ref(self.data_indexes_ref)


class Response(SQLAlchemyObjectType):

    values = graphene.List(graphene.Float)
    name = graphene.String()
    data_ref = graphene.String()

    class Meta:
        model = ResponseModel
        exclude_fields = ("values_ref",)

    def resolve_values(self, info):
        return fetch_blob_from_ref(self.values_ref)

    def resolve_name(self, info):
        pd = ResponseDefinition.get_query(info).get(self.response_definition_id)
        return pd.name

    def resolve_data_ref(self, info):
        return f"/data/{self.values_ref}"


class Parameter(SQLAlchemyObjectType):

    value = graphene.Float()
    name = graphene.String()

    class Meta:
        model = ParameterModel
        exclude_fields = ("value_ref",)

    def resolve_value(self, info):
        return fetch_blob_from_ref(self.value_ref)

    def resolve_name(self, info):
        pd = ParameterDefinition.get_query(info).get(self.parameter_definition_id)
        return pd.name


class Realization(SQLAlchemyObjectType):
    response = graphene.List(Response, name=graphene.String(required=True))
    response_data_ref = graphene.String(name=graphene.String(required=True))
    parameter = graphene.List(Parameter, name=graphene.String(required=True))
    parameter_data_ref = graphene.String(name=graphene.String(required=True))

    class Meta:
        model = RealizationModel

    def resolve_response(self, info, name=None):
        response_definition = (
            ResponseDefinition.get_query(info)
            .filter_by(ensemble_id=self.ensemble_id, name=name)
            .first()
        )
        return (
            Response.get_query(info)
            .filter_by(
                realization_id=self.id, response_definition_id=response_definition.id
            )
            .all()
        )

    def resolve_parameter(self, info, name=None):
        parameter_definition = (
            ParameterDefinition.get_query(info)
            .filter_by(ensemble_id=self.ensemble_id, name=name)
            .first()
        )

        return (
            Parameter.get_query(info)
            .filter_by(
                realization_id=self.id, parameter_definition_id=parameter_definition.id
            )
            .all()
        )


class Ensemble(SQLAlchemyObjectType):
    update_source = graphene.String()
    response = graphene.List(Response, name=graphene.String(required=True))
    response_data_ref = graphene.String(
        name=graphene.String(required=True), level=graphene.Int(required=False)
    )

    parameter = graphene.List(Parameter, name=graphene.String(required=True))
    parameter_data_ref = graphene.String(name=graphene.String(required=True))

    class Meta:
        model = EnsembleModel

    def resolve_update_source(self, info):
        if self.parent is not None:
            return self.parent.ensemble_reference.name
        return None

    def resolve_response(self, info, name=None):
        response_definition = (
            ResponseDefinition.get_query(info)
            .filter_by(ensemble_id=self.id, name=name)
            .first()
        )
        return (
            Response.get_query(info)
            .filter_by(response_definition_id=response_definition.id)
            .all()
        )

    def resolve_response_data_ref(self, info, name, level):
        if level == 0:
            return f"/ensembles/{self.id}/responses/{name}/data"
        else:
            with StorageApi(rdb_url=rdb_url, blob_url=blob_url) as api:
                ids = api.get_response_data(self.id, name)
                data_list = [data for data in api.get_data(ids)]
                df = pd.DataFrame(data=data_list).transpose()
                # we need to do simplification of df here and store it as temporal blob and provide endpoint
            return f"/ensembles/{self.id}/responses/{name}/data/{level}"

    def resolve_parameter_data_ref(self, info, name):
        parameter_definition = (
            ParameterDefinition.get_query(info)
            .filter_by(ensemble_id=self.id, name=name)
            .first()
        )
        return f"/ensembles/{self.id}/responses/{parameter_definition.id}/data"

    def resolve_parameter(self, info, name=None):
        parameter_definition = (
            ParameterDefinition.get_query(info)
            .filter_by(ensemble_id=self.id, name=name)
            .first()
        )

        return (
            Parameter.get_query(info)
            .filter_by(parameter_definition_id=parameter_definition.id)
            .all()
        )


class ResponseDefinition(SQLAlchemyObjectType):
    indexes = graphene.List(graphene.Int)
    observations = graphene.List(of_type=Observation)

    class Meta:
        model = ResponseDefinitionModel
        exclude_fields = ("indexes_ref", "observation_links")

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


class _ObsResDefLink(SQLAlchemyObjectType):
    class Meta:
        model = ObsResDefLinkModel


class Query(graphene.ObjectType):
    ensemble = graphene.Field(
        Ensemble, id=graphene.Int(required=False), name=graphene.String(required=False)
    )
    all_ensembles = graphene.List(Ensemble)

    def resolve_ensemble(self, info, idx=None, name=None):
        if idx is not None:
            return Ensemble.get_query(info).filter_by(id=id).first()
        elif name is not None:
            return Ensemble.get_query(info).filter_by(name=name).first()

    def resolve_all_ensembles(self, info):
        return Ensemble.get_query(info).all()


schema = graphene.Schema(
    query=Query,
    types=[Ensemble, Realization, ResponseDefinition, Parameter, Response, Observation],
)
