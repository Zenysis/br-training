"""Add data export field to role model. Also adds new permissions for DQL and CM

Revision ID: 93bb8d693499
Revises: 25faffac83d1
Create Date: 2020-06-02 15:38:41.914595

"""
from alembic import op
import sqlalchemy as sa

from web.server.migrations.seed_scripts.seed_93bb8d693499_add_data_export_field_to_role import (
    upvert_data,
    downvert_data,
)


# revision identifiers, used by Alembic.
revision = '93bb8d693499'
down_revision = '25faffac83d1'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('role', schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                'enable_data_export',
                sa.Boolean(),
                server_default='false',
                nullable=False,
            )
        )

    upvert_data(op)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    downvert_data(op)

    with op.batch_alter_table('role', schema=None) as batch_op:
        batch_op.drop_column('enable_data_export')
    # ### end Alembic commands ###
