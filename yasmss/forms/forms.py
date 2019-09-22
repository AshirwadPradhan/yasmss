from flask_wtf import FlaskForm
from wtforms import TextAreaField, SubmitField
# from wtforms.validators import DataRequired

class QueryForm(FlaskForm):
    query = TextAreaField('')
    submit = SubmitField('submit')