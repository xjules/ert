from flask import Flask
from flask_graphql import GraphQLView

from ert_shared.storage.graphql_schema import db_session
from ert_shared.storage.graphql_schema import schema, EnsembleModel

app = Flask(__name__)
app.debug = True

app.add_url_rule(
    '/graphql',
    view_func=GraphQLView.as_view(
        'graphql',
        schema=schema,
        graphiql=True  # for having the GraphiQL interface
    )
)


@app.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()


if __name__ == '__main__':
    app.run()
