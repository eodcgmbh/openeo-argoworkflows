"""initialise-db

Revision ID: 28fe2ce196c8
Revises: 
Create Date: 2024-06-07 07:34:42.413410

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy import Text

# revision identifiers, used by Alembic.
revision: str = '28fe2ce196c8'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('jobs',
    sa.Column('job_id', sa.UUID(), nullable=False),
    sa.Column('process', postgresql.JSON(astext_type=Text()), nullable=False),
    sa.Column('status', postgresql.ENUM('created', 'queued', 'running', 'canceled', 'finished', 'error', name='status'), nullable=False),
    sa.Column('user_id', sa.UUID(), nullable=False),
    sa.Column('created', sa.DateTime(), nullable=False),
    sa.Column('title', sa.VARCHAR(), nullable=True),
    sa.Column('description', sa.VARCHAR(), nullable=True),
    sa.Column('synchronous', sa.BOOLEAN(), nullable=False),
    sa.Column('workflowname', sa.VARCHAR(), nullable=True),
    sa.PrimaryKeyConstraint('job_id')
    )
    op.create_table('udps',
    sa.Column('id', sa.VARCHAR(), nullable=False),
    sa.Column('user_id', sa.UUID(), nullable=False),
    sa.Column('process_graph', postgresql.JSON(astext_type=Text()), nullable=False),
    sa.Column('created', sa.DateTime(), nullable=False),
    sa.Column('parameters', postgresql.JSON(astext_type=Text()), nullable=True),
    sa.Column('returns', postgresql.JSON(astext_type=Text()), nullable=True),
    sa.Column('summary', sa.VARCHAR(), nullable=True),
    sa.Column('description', sa.VARCHAR(), nullable=True),
    sa.PrimaryKeyConstraint('id', 'user_id')
    )
    op.create_table('users',
    sa.Column('user_id', sa.UUID(), nullable=False),
    sa.Column('oidc_sub', sa.VARCHAR(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.PrimaryKeyConstraint('user_id'),
    sa.UniqueConstraint('oidc_sub'),
    sa.UniqueConstraint('user_id')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('users')
    op.drop_table('udps')
    op.drop_table('jobs')
    # ### end Alembic commands ###
