from os import getenv
from flask import Flask
from mongoengine import connect
import click
from pipeline.pipeline import Pipeline
from dotenv import load_dotenv


def create_app():
    """
    Boostrap file
    :return: flask application
    """
    load_dotenv()

    config = {
        "DEBUG": getenv('debug', False),
        'TOTAL_DATA_RANGE': getenv('TOTAL_DATA_RANGE'),
        'CROWDSTRIKE_ENDPOINT': getenv('CROWDSTRIKE_ENDPOINT'),
        'QUALYS_ENDPOINT': getenv('QUALYS_ENDPOINT'),
        'SECRET': getenv('SECRET'),
        'MONGO_URL': getenv('MONGO_URL'),
        'MONGO_DATABASE': getenv('MONGO_DATABASE'),
    }
    connect(host=config['MONGO_URL'], db=config['MONGO_DATABASE'],)

    app = Flask(__name__)

    from .routes import main_blueprint
    app.register_blueprint(main_blueprint)

    app.config.from_mapping(config)

    with app.app_context():
        @app.cli.command('pipeline')
        @click.option('-sc', '--skip_collecting', type=bool)
        @click.option('-sa', '--skip_aggregating', type=bool)
        @click.option('-exit', '--exit_on_failure', type=bool)
        @click.option('-s', '--skip', type=int)
        @click.option('-l', '--limit', type=int)
        @click.option('-t', '--total', type=int)
        def pipeline(skip_collecting, skip_aggregating, exit_on_failure, skip, limit, total) -> None:
            click.secho('Pipeline', fg='green', bold=True)
            model = Pipeline()
            model.process({
                'skip_collecting': skip_collecting or False,
                'skip_aggregating': skip_aggregating or False,
                'exit_on_failure': exit_on_failure or False,
                'skip': skip or 0,
                'limit': limit or 2,
                'total': total or 10,
            })
            click.secho('Completed', fg='green', bold=True)
    return app
