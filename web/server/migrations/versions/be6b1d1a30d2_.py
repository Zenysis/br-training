"""empty message

Revision ID: be6b1d1a30d2
Revises: 05cc6c8d7b0a
Create Date: 2020-04-30 22:54:52.215342

"""
from alembic import op
from sqlalchemy.schema import Sequence, CreateSequence, DropSequence
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'be6b1d1a30d2'
down_revision = '05cc6c8d7b0a'
branch_labels = None
depends_on = None


def upgrade():
    op.execute(CreateSequence(Sequence('case_type_default_druid_dimension_id_seq')))
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table(
        'case_type_default_druid_dimension', schema=None
    ) as batch_op:
        batch_op.add_column(
            sa.Column(
                'id',
                sa.Integer(),
                nullable=False,
                server_default=sa.text(
                    "nextval('case_type_default_druid_dimension_id_seq'::regclass)"
                ),
            )
        )
        batch_op.drop_constraint('case_type_default_druid_dimension_pkey')
        batch_op.create_primary_key('case_type_default_druid_dimension_pkey', ['id'])

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table(
        'case_type_default_druid_dimension', schema=None
    ) as batch_op:
        batch_op.drop_constraint('case_type_default_druid_dimension_pkey')
        batch_op.create_primary_key(
            'case_type_default_druid_dimension_pkey',
            ['case_type_id', 'druid_dimension_name'],
        )
        batch_op.drop_column('id')
    op.execute(DropSequence(Sequence('case_type_default_druid_dimension_id_seq')))

    # ### end Alembic commands ###
