"""field reported_operation_status

Revision ID: fb5ed9fae5e7
Revises: 9e9348366a3b
Create Date: 2022-06-15 14:07:24.896473

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'fb5ed9fae5e7'
down_revision = '9e9348366a3b'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('eo_DB', schema=None) as batch_op:
        batch_op.add_column(sa.Column('reported_operation_finish_date', sa.DateTime(), nullable=True))

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('eo_DB', schema=None) as batch_op:
        batch_op.drop_column('reported_operation_finish_date')

    # ### end Alembic commands ###
