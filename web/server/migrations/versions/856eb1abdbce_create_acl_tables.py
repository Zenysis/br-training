"""Creates ACL, ResourceRole, ResourceRolePermission tables

Revision ID: 856eb1abdbce
Revises: 9a485a630d00
Create Date: 2020-03-10 16:05:55.852840

Summary: This migration does the following:
    1. Creates ResourceRole and ResourceRolePermissions table and populates this
        from Role table.
    2. Creates UserAcl and GroupAcl tables that replace resource specific rows
        in the Role table and links to corresponding entries in ResourceRole.

"""
from alembic import op
import sqlalchemy as sa

from web.server.migrations.seed_scripts.seed_856eb1abdbce_create_acl_tables import (
    upvert_data,
    downvert_data,
)

# revision identifiers, used by Alembic.
revision = '856eb1abdbce'
down_revision = 'ca5ccf4e8a59'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        'resource_role',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('name', sa.String(length=50), server_default='', nullable=False),
        sa.Column('resource_type_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ['resource_type_id'],
            ['resource_type.id'],
            name='valid_resource_type',
            ondelete='RESTRICT',
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name'),
    )
    op.create_table(
        'resource_role_permission',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('resource_role_id', sa.Integer(), nullable=False),
        sa.Column('permission_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ['permission_id'],
            ['permission.id'],
            name='valid_permission',
            ondelete='CASCADE',
        ),
        sa.ForeignKeyConstraint(
            ['resource_role_id'], ['resource_role.id'], ondelete='CASCADE'
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_table(
        'security_group_acl',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('group_id', sa.Integer(), nullable=False),
        sa.Column('resource_id', sa.Integer(), nullable=True),
        sa.Column('resource_role_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ['group_id'],
            ['security_group.id'],
            name='valid_security_group',
            ondelete='CASCADE',
        ),
        sa.ForeignKeyConstraint(
            ['resource_id'], ['resource.id'], name='valid_resource', ondelete='CASCADE'
        ),
        sa.ForeignKeyConstraint(
            ['resource_role_id'],
            ['resource_role.id'],
            name='valid_resource_role',
            ondelete='CASCADE',
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_table(
        'user_acl',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('resource_id', sa.Integer(), nullable=False),
        sa.Column('resource_role_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ['resource_id'], ['resource.id'], name='valid_resource', ondelete='CASCADE'
        ),
        sa.ForeignKeyConstraint(
            ['resource_role_id'],
            ['resource_role.id'],
            name='valid_resource_role',
            ondelete='CASCADE',
        ),
        sa.ForeignKeyConstraint(
            ['user_id'], ['user.id'], name='valid_user', ondelete='CASCADE'
        ),
        sa.PrimaryKeyConstraint('id'),
    )

    upvert_data(op)

    with op.batch_alter_table('security_group_roles', schema=None) as batch_op:
        batch_op.drop_constraint('valid_resource', type_='foreignkey')
        batch_op.drop_column('resource_id')

    with op.batch_alter_table('user_roles', schema=None) as batch_op:
        batch_op.drop_constraint('valid_resource', type_='foreignkey')
        batch_op.drop_column('resource_id')

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('user_roles', schema=None) as batch_op:
        batch_op.add_column(
            sa.Column('resource_id', sa.INTEGER(), autoincrement=False, nullable=True)
        )
        batch_op.create_foreign_key(
            'valid_resource', 'resource', ['resource_id'], ['id'], ondelete='CASCADE'
        )

    with op.batch_alter_table('security_group_roles', schema=None) as batch_op:
        batch_op.add_column(
            sa.Column('resource_id', sa.INTEGER(), autoincrement=False, nullable=True)
        )
        batch_op.create_foreign_key(
            'valid_resource', 'resource', ['resource_id'], ['id'], ondelete='CASCADE'
        )

    downvert_data(op)

    op.drop_table('user_acl')
    op.drop_table('security_group_acl')
    op.drop_table('resource_role_permission')
    op.drop_table('resource_role')
    # ### end Alembic commands ###
