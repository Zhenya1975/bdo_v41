"""mironov columns

Revision ID: 6bf75fc7cff4
Revises: dc1e90624834
Create Date: 2022-07-08 11:41:19.742946

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6bf75fc7cff4'
down_revision = 'dc1e90624834'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('eo_DB', schema=None) as batch_op:
        batch_op.add_column(sa.Column('type_mironov', sa.String(), nullable=True))
        batch_op.add_column(sa.Column('short_description_mironov', sa.String(), nullable=True))
        batch_op.add_column(sa.Column('marka_modeli_mironov', sa.String(), nullable=True))
        batch_op.add_column(sa.Column('marka_oborudovania_mironov', sa.String(), nullable=True))

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('eo_DB', schema=None) as batch_op:
        batch_op.drop_column('marka_oborudovania_mironov')
        batch_op.drop_column('marka_modeli_mironov')
        batch_op.drop_column('short_description_mironov')
        batch_op.drop_column('type_mironov')

    # ### end Alembic commands ###
