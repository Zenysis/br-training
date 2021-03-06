"""Add DataUploadFileSummary (data_upload_file_summary) table.

Revision ID: e1e665f992c7
Revises: 02d20fb0c3ed
Create Date: 2021-03-15 21:40:54.481794

"""
# pylint: disable-all
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'e1e665f992c7'
down_revision = '02d20fb0c3ed'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        'data_upload_file_summary',
        sa.Column(
            'created', sa.DateTime(), server_default=sa.text('now()'), nullable=False
        ),
        sa.Column(
            'last_modified',
            sa.DateTime(),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('source_id', sa.String(length=100), nullable=True),
        sa.Column('file_path', sa.Text(), nullable=True),
        sa.Column(
            'column_mapping', postgresql.JSONB(astext_type=sa.Text()), nullable=True
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('data_upload_file_summary')
    # ### end Alembic commands ###
