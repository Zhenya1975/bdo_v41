"""init

Revision ID: 3fc899108b93
Revises: 07017e254730
Create Date: 2022-07-11 08:11:41.858172

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '3fc899108b93'
down_revision = '07017e254730'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('eo_DB', schema=None) as batch_op:
        batch_op.add_column(sa.Column('constr_type', sa.String(), nullable=True))
        batch_op.add_column(sa.Column('constr_type_descr', sa.String(), nullable=True))
        batch_op.drop_column('short_description_mironov')
        batch_op.drop_column('type_mironov')
        batch_op.drop_column('marka_oborudovania_mironov')
        batch_op.drop_column('marka_modeli_mironov')

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('eo_DB', schema=None) as batch_op:
        batch_op.add_column(sa.Column('marka_modeli_mironov', sa.VARCHAR(), nullable=True))
        batch_op.add_column(sa.Column('marka_oborudovania_mironov', sa.VARCHAR(), nullable=True))
        batch_op.add_column(sa.Column('type_mironov', sa.VARCHAR(), nullable=True))
        batch_op.add_column(sa.Column('short_description_mironov', sa.VARCHAR(), nullable=True))
        batch_op.drop_column('constr_type_descr')
        batch_op.drop_column('constr_type')

    # ### end Alembic commands ###