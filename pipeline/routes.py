from flask import Blueprint, render_template

main_blueprint = Blueprint('main', __name__)


@main_blueprint.route('/', methods=['GET'])
def index() -> str:
    """
    Index web page
    :return: str
    """
    from .report import Report
    report = Report()
    return render_template('index/index.html', data=report.index())
