"""add-roles-to-users

Revision ID: 3a1f9e0d4b72
Revises: 28fe2ce196c8
Create Date: 2026-04-28 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '3a1f9e0d4b72'
down_revision: Union[str, None] = '28fe2ce196c8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'users',
        sa.Column('roles', postgresql.ARRAY(sa.VARCHAR()), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('users', 'roles')
