''' Drop Pending User Table and introduce concept of User Status

Revision ID: b2690a565224
Revises: 010bef7d6239
Create Date: 2018-05-02 20:28:04.035374

'''
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ENUM

from web.server.migrations.seed_scripts.seed_b2690a565224 import (upvert_data, downvert_data)

# revision identifiers, used by Alembic.
revision = 'b2690a565224'
down_revision = '1b25c9c0b5dc'
branch_labels = None
depends_on = None

user_status_enum = ENUM('ACTIVE', 'INACTIVE', 'PENDING', name='userstatusenum', create_type=False)

def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    user_status_enum.create(op.get_bind(), checkfirst=True)
    op.create_table('user_status',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('status', user_status_enum, nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('status')
    )

    op.add_column('user', sa.Column('status_id', sa.Integer()))
    upvert_data(op)

    with op.batch_alter_table('user', schema=None) as batch_op:
        batch_op.alter_column(sa.Column('status_id', sa.Integer(), nullable=False))
        batch_op.create_foreign_key('valid_status', 'user_status', ['status_id'], ['id'], ondelete='RESTRICT')
        batch_op.drop_column('is_active')

    op.drop_table('pending_user')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('pending_user',
    sa.Column('id', sa.INTEGER(), nullable=False),
    sa.Column('username', sa.VARCHAR(length=50), autoincrement=False, nullable=False),
    sa.Column('first_name', sa.VARCHAR(length=100), server_default=sa.text(u"''::character varying"), autoincrement=False, nullable=False),
    sa.Column('last_name', sa.VARCHAR(length=100), server_default=sa.text(u"''::character varying"), autoincrement=False, nullable=False),
    sa.Column('invite_token', sa.VARCHAR(length=100), server_default=sa.text(u"''::character varying"), autoincrement=False, nullable=False),
    sa.PrimaryKeyConstraint('id', name=u'pending_user_pkey'),
    sa.UniqueConstraint('username', name=u'pending_user_username_key')
    )

    op.add_column('user', sa.Column('is_active', sa.BOOLEAN(), server_default=sa.text(u'true'), autoincrement=False))
    downvert_data(op)

    with op.batch_alter_table('user', schema=None) as batch_op:
        batch_op.alter_column(sa.Column('is_active', sa.Integer(), nullable=False))
        batch_op.drop_constraint('valid_status', type_='foreignkey')
        batch_op.drop_column('status_id')

    op.drop_table('user_status')
    user_status_enum.drop(op.get_bind(), checkfirst=True)
    # ### end Alembic commands ###