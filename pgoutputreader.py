import argparse
import io
import json
import logging
import typing
import uuid
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Union

import psycopg2
import psycopg2.extensions
import psycopg2.extras

logger = logging.getLogger('pgoutput-reader')

# integer byte lengths
INT8 = 1
INT16 = 2
INT32 = 4
INT64 = 8


class PgoutputError(Exception):
    pass


class QueryError(PgoutputError):
    pass


class ResourceError(PgoutputError):
    pass


class StopConsume(PgoutputError):
    pass


class SourceDBHandler:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.connect()

    def connect(self) -> None:
        self.conn = psycopg2.connect(self.dsn)
        self.conn.autocommit = True

    def fetchone(self, query: str) -> psycopg2.extras.DictRow:
        try:
            cursor = psycopg2.extras.DictCursor(self.conn)
        except Exception as err:
            raise ResourceError("Could not get cursor") from err
        try:
            cursor.execute(query)
            result: psycopg2.extras.DictRow = cursor.fetchone()
            return result
        except Exception as err:
            self.conn.rollback()
            raise QueryError("Error running query") from err
        finally:
            cursor.close()

    def fetch(self, query: str) -> List[psycopg2.extras.DictRow]:
        try:
            cursor = psycopg2.extras.DictCursor(self.conn)
        except Exception as err:
            raise ResourceError("Could not get cursor") from err
        try:
            cursor.execute(query)
            result: List[psycopg2.extras.DictRow] = cursor.fetchall()
            return result
        except Exception as err:
            self.conn.rollback()
            raise QueryError("Error running query") from err
        finally:
            cursor.close()

    def fetch_column_type(self, type_id: int, atttypmod: int) -> str:
        """Get formatted data type name"""
        query = f"SELECT format_type({type_id}, {atttypmod}) AS data_type"
        result = self.fetchone(query=query)
        return result["data_type"]

    def fetch_if_column_is_optional(self, table_schema: str, table_name: str, column_name: str) -> bool:
        """Check if a column is optional"""
        query = f"""SELECT attnotnull
            FROM pg_attribute
            WHERE attrelid = '{table_schema}.{table_name}'::regclass
            AND attname = '{column_name}';
        """
        result = self.fetchone(query=query)
        # attnotnull returns if column has not null constraint, we want to flip it
        return False if result["attnotnull"] else True

    def close(self) -> None:
        self.conn.close()


@dataclass(frozen=True)
class ColumnData:
    # col_data_category is NOT the type. it means null value/toasted(not sent)/text formatted
    col_data_category: Optional[str]
    col_data_length: Optional[int] = None
    col_data: Optional[str] = None

    def __repr__(self) -> str:
        return f"[col_data_category='{self.col_data_category}', col_data_length={self.col_data_length}, col_data='{self.col_data}']"


@dataclass(frozen=True)
class ColumnType:
    """https://www.postgresql.org/docs/12/catalog-pg-attribute.html"""

    part_of_pkey: int
    name: str
    type_id: int
    atttypmod: int


@dataclass(frozen=True)
class TupleData:
    n_columns: int
    column_data: List[ColumnData]

    def __repr__(self) -> str:
        return f"n_columns: {self.n_columns}, data: {self.column_data}"


def convert_pg_ts(_ts_in_microseconds: int) -> datetime:
    ts = datetime(2000, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
    return ts + timedelta(microseconds=_ts_in_microseconds)


def convert_bytes_to_int(_in_bytes: bytes) -> int:
    return int.from_bytes(_in_bytes, byteorder="big", signed=True)


def convert_bytes_to_utf8(_in_bytes: Union[bytes, bytearray]) -> str:
    return (_in_bytes).decode("utf-8")


class PgoutputMessage(ABC):
    def __init__(self, buffer: bytes):
        self.buffer: io.BytesIO = io.BytesIO(buffer)
        self.byte1: str = self.read_utf8(1)
        self.decode_buffer()

    @abstractmethod
    def decode_buffer(self) -> None:
        """Decoding is implemented for each message type"""

    @abstractmethod
    def __repr__(self) -> str:
        """Implemented for each message type"""

    def read_int8(self) -> int:
        return convert_bytes_to_int(self.buffer.read(INT8))

    def read_int16(self) -> int:
        return convert_bytes_to_int(self.buffer.read(INT16))

    def read_int32(self) -> int:
        return convert_bytes_to_int(self.buffer.read(INT32))

    def read_int64(self) -> int:
        return convert_bytes_to_int(self.buffer.read(INT64))

    def read_utf8(self, n: int = 1) -> str:
        return convert_bytes_to_utf8(self.buffer.read(n))

    def read_timestamp(self) -> datetime:
        # 8 chars -> int64 -> timestamp
        return convert_pg_ts(_ts_in_microseconds=self.read_int64())

    def read_string(self) -> str:
        output = bytearray()
        while True:
            next_char = self.buffer.read(1)
            if next_char == b"\x00":
                break
            output += next_char
        return convert_bytes_to_utf8(output)

    def read_tuple_data(self) -> TupleData:
        """
        TupleData
        Int16  Number of columns.
        Next, one of the following submessages appears for each column (except generated columns):
                Byte1('n') Identifies the data as NULL value.
            Or
                Byte1('u') Identifies unchanged TOASTed value (the actual value is not sent).
            Or
                Byte1('t') Identifies the data as text formatted value.
                Int32 Length of the column value.
                Byten The value of the column, in text format. (A future release might support additional formats.) n is the above length.
        """
        # TODO: investigate what happens with the generated columns
        column_data = list()
        n_columns = self.read_int16()
        for column in range(n_columns):
            col_data_category = self.read_utf8()
            if col_data_category in ("n", "u"):
                # "n"=NULL, "t"=TOASTed
                column_data.append(ColumnData(col_data_category=col_data_category))
            elif col_data_category == "t":
                # t = tuple
                col_data_length = self.read_int32()
                col_data = self.read_utf8(col_data_length)
                column_data.append(
                    ColumnData(
                        col_data_category=col_data_category,
                        col_data_length=col_data_length,
                        col_data=col_data,
                    )
                )
        return TupleData(n_columns=n_columns, column_data=column_data)


class Begin(PgoutputMessage):
    """
    https://pgpedia.info/x/xlogrecptr.html
    https://www.postgresql.org/docs/14/datatype-pg-lsn.html

    byte1 Byte1('B') Identifies the message as a begin message.
    lsn Int64 The final LSN of the transaction.
    commit_tx_ts Int64 Commit timestamp of the transaction. The value is in number of microseconds since PostgreSQL epoch (2000-01-01).
    tx_xid Int32 Xid of the transaction.
    """

    byte1: str
    lsn: int
    commit_ts: datetime
    tx_xid: int

    def decode_buffer(self) -> None:
        if self.byte1 != "B":
            raise ValueError("first byte in buffer does not match Begin message")
        self.lsn = self.read_int64()
        self.commit_ts = self.read_timestamp()
        self.tx_xid = self.read_int64()

    def __repr__(self) -> str:
        return (
            f"BEGIN \n\tbyte1: '{self.byte1}', \n\tLSN: {self.lsn}, "
            f"\n\tcommit_ts {self.commit_ts}, \n\ttx_xid: {self.tx_xid}"
        )


class Commit(PgoutputMessage):
    """
    byte1: Byte1('C') Identifies the message as a commit message.
    flags: Int8 Flags; currently unused (must be 0).
    lsn_commit: Int64 The LSN of the commit.
    lsn: Int64 The end LSN of the transaction.
    Int64 Commit timestamp of the transaction. The value is in number of microseconds since PostgreSQL epoch (2000-01-01).
    """

    byte1: str
    flags: int
    lsn_commit: int
    lsn: int
    commit_ts: datetime

    def decode_buffer(self) -> None:
        if self.byte1 != "C":
            raise ValueError("first byte in buffer does not match Commit message")
        self.flags = self.read_int8()
        self.lsn_commit = self.read_int64()
        self.lsn = self.read_int64()
        self.commit_ts = self.read_timestamp()

    def __repr__(self) -> str:
        return (
            f"COMMIT \n\tbyte1: {self.byte1}, \n\tflags {self.flags}, \n\tlsn_commit: {self.lsn_commit}"
            f"\n\tLSN: {self.lsn}, \n\tcommit_ts {self.commit_ts}"
        )


class Origin:
    """
    Byte1('O') Identifies the message as an origin message.
    Int64  The LSN of the commit on the origin server.
    String Name of the origin.
    Note that there can be multiple Origin messages inside a single transaction.
    This seems to be what origin means: https://www.postgresql.org/docs/12/replication-origins.html
    """

    pass


class Relation(PgoutputMessage):
    """
    Byte1('R')  Identifies the message as a relation message.
    Int32 ID of the relation.
    String Namespace (empty string for pg_catalog).
    String Relation name.
    Int8 Replica identity setting for the relation (same as relreplident in pg_class).
        # select relreplident from pg_class where relname = 'test_table';
        # from reading the documentation and looking at the tables this is not int8 but a single character
        # background: https://www.postgresql.org/docs/10/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY
    Int16 Number of columns.
    Next, the following message part appears for each column (except generated columns):
        Int8 Flags for the column. Currently can be either 0 for no flags or 1 which marks the column as part of the key.
        String Name of the column.
        Int32 ID of the column's data type.
        Int32 Type modifier of the column (atttypmod).
    """

    byte1: str
    relation_id: int
    namespace: str
    relation_name: str
    replica_identity_setting: str
    n_columns: int
    columns: List[ColumnType]

    def decode_buffer(self) -> None:
        if self.byte1 != "R":
            raise ValueError("first byte in buffer does not match Relation message")
        self.relation_id = self.read_int32()
        self.namespace = self.read_string()
        self.relation_name = self.read_string()
        self.replica_identity_setting = self.read_utf8()
        self.n_columns = self.read_int16()
        self.columns = list()

        for column in range(self.n_columns):
            part_of_pkey = self.read_int8()
            col_name = self.read_string()
            data_type_id = self.read_int32()
            # TODO: check on use of signed / unsigned
            # check with select oid from pg_type where typname = <type>; timestamp == 1184, int4 = 23
            col_modifier = self.read_int32()
            self.columns.append(
                ColumnType(
                    part_of_pkey=part_of_pkey,
                    name=col_name,
                    type_id=data_type_id,
                    atttypmod=col_modifier,
                )
            )

    def __repr__(self) -> str:
        return (
            f"RELATION \n\tbyte1: '{self.byte1}', \n\trelation_id: {self.relation_id}"
            f",\n\tnamespace/schema: '{self.namespace}',\n\trelation_name: '{self.relation_name}'"
            f",\n\treplica_identity_setting: '{self.replica_identity_setting}',\n\tn_columns: {self.n_columns} "
            f",\n\tcolumns: {self.columns}"
        )


class PgType:
    """
    Renamed to PgType not to collide with "type"

    Byte1('Y') Identifies the message as a type message.
    Int32 ID of the data type.
    String Namespace (empty string for pg_catalog).
    String Name of the data type.
    """

    pass


class Insert(PgoutputMessage):
    """
    Byte1('I')  Identifies the message as an insert message.
    Int32 ID of the relation corresponding to the ID in the relation message.
    Byte1('N') Identifies the following TupleData message as a new tuple.
    TupleData TupleData message part representing the contents of new tuple.
    """

    byte1: str
    relation_id: int
    new_tuple_byte: str
    new_tuple: TupleData

    def decode_buffer(self) -> None:
        if self.byte1 != "I":
            raise ValueError(f"first byte in buffer does not match Insert message (expected 'I', got '{self.byte1}'")
        self.relation_id = self.read_int32()
        self.new_tuple_byte = self.read_utf8()
        self.new_tuple = self.read_tuple_data()

    def __repr__(self) -> str:
        return (
            f"INSERT \n\tbyte1: '{self.byte1}', \n\trelation_id: {self.relation_id} "
            f"\n\tnew tuple byte: '{self.new_tuple_byte}', \n\tnew_tuple: {self.new_tuple}"
        )


class Update(PgoutputMessage):
    """
    Byte1('U')      Identifies the message as an update message.
    Int32           ID of the relation corresponding to the ID in the relation message.
    Byte1('K')      Identifies the following TupleData submessage as a key. This field is optional and is only present if the update changed data in any of the column(s) that are part of the REPLICA IDENTITY index.
    Byte1('O')      Identifies the following TupleData submessage as an old tuple. This field is optional and is only present if table in which the update happened has REPLICA IDENTITY set to FULL.
    TupleData       TupleData message part representing the contents of the old tuple or primary key. Only present if the previous 'O' or 'K' part is present.
    Byte1('N')      Identifies the following TupleData message as a new tuple.
    TupleData       TupleData message part representing the contents of a new tuple.

    The Update message may contain either a 'K' message part or an 'O' message part or neither of them, but never both of them.
    """

    byte1: str
    relation_id: int
    next_byte_identifier: Optional[str]
    optional_tuple_identifier: Optional[str]
    old_tuple: Optional[TupleData]
    new_tuple_byte: str
    new_tuple: TupleData

    def decode_buffer(self) -> None:
        self.optional_tuple_identifier = None
        self.old_tuple = None
        if self.byte1 != "U":
            raise ValueError(f"first byte in buffer does not match Update message (expected 'U', got '{self.byte1}'")
        self.relation_id = self.read_int32()
        # TODO test update to PK, test update with REPLICA IDENTITY = FULL
        self.next_byte_identifier = self.read_utf8()  # one of K, O or N
        if self.next_byte_identifier == "K" or self.next_byte_identifier == "O":
            self.optional_tuple_identifier = self.next_byte_identifier
            self.old_tuple = self.read_tuple_data()
            self.new_tuple_byte = self.read_utf8()
        else:
            self.new_tuple_byte = self.next_byte_identifier
        if self.new_tuple_byte != "N":
            # TODO: test exception handling
            raise ValueError(
                f"did not find new_tuple_byte ('N') at position: {self.buffer.tell()}, found: '{self.new_tuple_byte}'"
            )
        self.new_tuple = self.read_tuple_data()

    def __repr__(self) -> str:
        return (
            f"UPDATE \n\tbyte1: '{self.byte1}', \n\trelation_id: {self.relation_id}"
            f"\n\toptional_tuple_identifier: '{self.optional_tuple_identifier}', \n\toptional_old_tuple_data: {self.old_tuple}"
            f"\n\tnew_tuple_byte: '{self.new_tuple_byte}', \n\tnew_tuple: {self.new_tuple}"
        )


class Delete(PgoutputMessage):
    """
    Byte1('D')      Identifies the message as a delete message.
    Int32           ID of the relation corresponding to the ID in the relation message.
    Byte1('K')      Identifies the following TupleData submessage as a key. This field is present if the table in which the delete has happened uses an index as REPLICA IDENTITY.
    Byte1('O')      Identifies the following TupleData message as a old tuple. This field is present if the table in which the delete has happened has REPLICA IDENTITY set to FULL.
    TupleData message part representing the contents of the old tuple or primary key, depending on the previous field.

    The Delete message may contain either a 'K' message part or an 'O' message part, but never both of them.
    """

    byte1: str
    relation_id: int
    message_type: str
    old_tuple: TupleData

    def decode_buffer(self) -> None:
        if self.byte1 != "D":
            raise ValueError(f"first byte in buffer does not match Delete message (expected 'D', got '{self.byte1}'")
        self.relation_id = self.read_int32()
        self.message_type = self.read_utf8()
        # TODO: test with replica identity full
        if self.message_type not in ["K", "O"]:
            raise ValueError(f"message type byte is not 'K' or 'O', got: '{self.message_type}'")
        self.old_tuple = self.read_tuple_data()

    def __repr__(self) -> str:
        return (
            f"DELETE \n\tbyte1: {self.byte1} \n\trelation_id: {self.relation_id} "
            f"\n\tmessage_type: {self.message_type} \n\told_tuple: {self.old_tuple}"
        )


class Truncate(PgoutputMessage):
    """
    Byte1('T')      Identifies the message as a truncate message.
    Int32           Number of relations
    Int8            Option bits for TRUNCATE: 1 for CASCADE, 2 for RESTART IDENTITY
    Int32           ID of the relation corresponding to the ID in the relation message. This field is repeated for each relation.
    """

    byte1: str
    number_of_relations: int
    option_bits: int
    relation_ids: List[int]

    def decode_buffer(self) -> None:
        if self.byte1 != "T":
            raise ValueError(f"first byte in buffer does not match Truncate message (expected 'T', got '{self.byte1}'")
        self.number_of_relations = self.read_int32()
        self.option_bits = self.read_int8()
        self.relation_ids = []
        for relation in range(self.number_of_relations):
            self.relation_ids.append(self.read_int32())

    def __repr__(self) -> str:
        return (
            f"TRUNCATE \n\tbyte1: {self.byte1} \n\tn_relations: {self.number_of_relations} "
            f"option_bits: {self.option_bits}, relation_ids: {self.relation_ids}"
        )


@dataclass
class ReplicationMessage:
    message_id: uuid.UUID
    data_start: int
    payload: bytes
    send_time: datetime
    data_size: int
    wal_end: int


@dataclass
class ColumnDefinition:
    name: str
    part_of_pkey: Union[bool, int]
    type_id: int
    type_name: str
    optional: bool


@dataclass
class TableSchema:
    column_definitions: typing.List[ColumnDefinition]
    db: str
    schema_name: str
    table: str
    relation_id: int


@dataclass
class Transaction:
    tx_id: int
    begin_lsn: int
    commit_ts: datetime


@dataclass
class ChangeEvent:
    op: str  # (ENUM of I, U, D, T)
    message_id: uuid.UUID
    lsn: int
    transaction: typing.Any  # Transaction  # replication/source metadata
    table_schema: typing.Any  # TableSchema
    before: typing.Any  # typing.Optional[typing.Dict[str, typing.Any]]  # depends on the source table
    after: typing.Any  # typing.Optional[typing.Dict[str, typing.Any]]


def map_tuple_to_dict(tuple_data: TupleData, relation: TableSchema) -> typing.OrderedDict[str, typing.Any]:
    """Convert tuple data to an OrderedDict with keys from relation mapped in order to tuple data"""
    output: typing.OrderedDict[str, typing.Any] = OrderedDict()
    for idx, col in enumerate(tuple_data.column_data):
        column_name = relation.column_definitions[idx].name
        output[column_name] = col.col_data
    return output


def convert_pg_type_to_py_type(pg_type_name):
    if pg_type_name == "bigint" or pg_type_name == "integer" or pg_type_name == "smallint":
        return int
    elif pg_type_name == "timestamp with time zone" or pg_type_name == "timestamp without time zone":
        return datetime
        # json not tested yet
    elif pg_type_name == "json" or pg_type_name == "jsonb":
        return dict
    elif pg_type_name[:7] == "numeric":
        return float
    else:
        return str


class LogicalReplicationReader:

    def __init__(
            self,
            publication_name: str,
            slot_name: str,
            dsn: typing.Optional[str] = None,
            send_feedback=False,
            process_result=None,
            auto_create=False,
            **kwargs: typing.Optional[str],
    ) -> None:
        self.dsn = psycopg2.extensions.make_dsn(dsn=dsn, **kwargs)
        self.database = psycopg2.extensions.parse_dsn(self.dsn)['dbname']
        self.publication_name = publication_name
        self.slot_name = slot_name

        # transform data containers
        self.table_schemas: typing.Dict[int, TableSchema] = dict()  # map relid to table schema

        # for each relation store pydantic model applied to be before/after tuple
        # key only is the schema for before messages that only contain the PK column changes
        self.key_only_table_models: typing.Dict[int, typing.Type[dict]] = dict()
        self.table_models: typing.Dict[int, typing.Type[dict]] = dict()

        # save map of type oid to readable name
        self.pg_types: typing.Dict[int, str] = dict()

        self.transaction_metadata = None
        self.source_db_handler = None
        self.replicat_conn = None
        self.replicat_cursor = None

        self.auto_create = auto_create
        self.send_feedback = send_feedback

        if process_result is None:
            def process_result(result):
                pass

        self.process_result = process_result

    def start(self):
        self.source_db_handler = SourceDBHandler(self.dsn)
        self.replicat_conn = psycopg2.extras.LogicalReplicationConnection(self.dsn)
        self.replicat_cursor = psycopg2.extras.ReplicationCursor(self.replicat_conn)
        replication_options = {"publication_names": self.publication_name, "proto_version": "1"}

        try:
            self.replicat_cursor.start_replication(slot_name=self.slot_name, decode=False, options=replication_options)
        except psycopg2.ProgrammingError:
            if not self.auto_create:
                raise
            logger.info('Replication slot %s does not exists, creating.', self.slot_name)
            self.replicat_cursor.create_replication_slot(self.slot_name, output_plugin="pgoutput")
            self.replicat_cursor.start_replication(slot_name=self.slot_name, decode=False, options=replication_options)
        try:
            logger.info(f"Starting replication from slot: {self.slot_name}")
            self.replicat_cursor.consume_stream(self.msg_consumer)
        except StopConsume:
            pass
        except Exception as err:
            logger.error(f"Error consuming stream from slot: '{self.slot_name}'. {err}")
        finally:
            self.replicat_cursor.close()
            self.replicat_conn.close()

    def msg_consumer(self, msg: psycopg2.extras.ReplicationMessage) -> None:
        message_id = uuid.uuid4()
        message = ReplicationMessage(
            message_id=message_id,
            data_start=msg.data_start,
            payload=msg.payload,
            send_time=msg.send_time,
            data_size=msg.data_size,
            wal_end=msg.wal_end,
        )

        for result in self.parse_message(message):
            should_stop = self.process_result(result)

            if self.send_feedback:
                if getattr(result, 'message_id', None) == message_id:
                    msg.cursor.send_feedback(flush_lsn=msg.data_start)
                    logger.debug(f"Flushed message: '{str(message_id)}', start at: {str(msg.data_start)}")
                else:
                    logger.warning(
                        f"Could not confirm message: {str(message_id)}. Did not flush at {str(msg.data_start)}")
                    
            if should_stop:
                raise StopConsume()

    def parse_message(self, msg):
        message_type = (msg.payload[:1]).decode("utf-8")
        if message_type == "R":
            self.process_relation(message=msg)
        elif message_type == "B":
            self.transaction_metadata = self.process_begin(message=msg)
        # message processors below will throw an error if transaction_metadata doesn't exist
        elif message_type == "I":
            yield self.process_insert(message=msg, transaction=self.transaction_metadata)
        elif message_type == "U":
            yield self.process_update(message=msg, transaction=self.transaction_metadata)
        elif message_type == "D":
            yield self.process_delete(message=msg, transaction=self.transaction_metadata)
        elif message_type == "T":
            yield from self.process_truncate(message=msg, transaction=self.transaction_metadata)
        elif message_type == "C":
            self.transaction_metadata = None  # null out this value after commit

    def process_relation(self, message: ReplicationMessage) -> None:
        relation_msg: Relation = Relation(message.payload)
        relation_id = relation_msg.relation_id
        column_definitions: typing.List[ColumnDefinition] = []
        for column in relation_msg.columns:
            self.pg_types[column.type_id] = self.source_db_handler.fetch_column_type(
                type_id=column.type_id, atttypmod=column.atttypmod
            )
            # pre-compute schema of the table for attaching to messages
            is_optional = self.source_db_handler.fetch_if_column_is_optional(
                table_schema=relation_msg.namespace, table_name=relation_msg.relation_name, column_name=column.name
            )
            column_definitions.append(
                ColumnDefinition(
                    name=column.name,
                    part_of_pkey=column.part_of_pkey,
                    type_id=column.type_id,
                    type_name=self.pg_types[column.type_id],
                    optional=is_optional,
                )
            )
        self.table_models[relation_id] = dict
        self.key_only_table_models[relation_id] = dict

        self.table_schemas[relation_id] = TableSchema(
            db=self.database,
            schema_name=relation_msg.namespace,
            table=relation_msg.relation_name,
            column_definitions=column_definitions,
            relation_id=relation_id,
        )

    def process_begin(self, message: ReplicationMessage) -> Transaction:
        begin_msg: Begin = Begin(message.payload)
        return Transaction(tx_id=begin_msg.tx_xid, begin_lsn=begin_msg.lsn, commit_ts=begin_msg.commit_ts)

    def process_insert(self, message: ReplicationMessage, transaction: Transaction) -> ChangeEvent:
        decoded_msg: Insert = Insert(message.payload)
        relation_id: int = decoded_msg.relation_id
        after = map_tuple_to_dict(tuple_data=decoded_msg.new_tuple, relation=self.table_schemas[relation_id])
        try:
            return ChangeEvent(
                op=decoded_msg.byte1,
                message_id=message.message_id,
                lsn=message.data_start,
                transaction=transaction,
                table_schema=self.table_schemas[relation_id],
                before=None,
                after=self.table_models[relation_id](**after),
            )
        except Exception as exc:
            raise PgoutputError(f"Error creating ChangeEvent: {exc}")

    def process_update(self, message: ReplicationMessage, transaction: Transaction) -> ChangeEvent:
        decoded_msg: Update = Update(message.payload)
        relation_id: int = decoded_msg.relation_id
        if decoded_msg.old_tuple:
            before_raw = map_tuple_to_dict(tuple_data=decoded_msg.old_tuple, relation=self.table_schemas[relation_id])
            if decoded_msg.optional_tuple_identifier == "O":
                before_typed = self.table_models[relation_id](**before_raw)
            # if there is old tuple and not O then key only schema needed
            else:
                before_typed = self.key_only_table_models[relation_id](**before_raw)
        else:
            before_typed = None
        after = map_tuple_to_dict(tuple_data=decoded_msg.new_tuple, relation=self.table_schemas[relation_id])
        try:
            return ChangeEvent(
                op=decoded_msg.byte1,
                message_id=message.message_id,
                lsn=message.data_start,
                transaction=transaction,
                table_schema=self.table_schemas[relation_id],
                before=before_typed,
                after=self.table_models[relation_id](**after),
            )
        except Exception as exc:
            raise PgoutputError(f"Error creating ChangeEvent: {exc}")

    def process_delete(self, message: ReplicationMessage, transaction: Transaction) -> ChangeEvent:
        decoded_msg: Delete = Delete(message.payload)
        relation_id: int = decoded_msg.relation_id
        before_raw = map_tuple_to_dict(tuple_data=decoded_msg.old_tuple, relation=self.table_schemas[relation_id])
        if decoded_msg.message_type == "O":
            # O is from REPLICA IDENTITY FULL and therefore has all columns in before message
            before_typed = self.table_models[relation_id](**before_raw)
        else:
            # message type is K and means only replica identity index is present in before tuple
            # only DEFAULT is implemented so the index can only be the primary key
            before_typed = self.key_only_table_models[relation_id](**before_raw)
        try:
            return ChangeEvent(
                op=decoded_msg.byte1,
                message_id=message.message_id,
                lsn=message.data_start,
                transaction=transaction,
                table_schema=self.table_schemas[relation_id],
                before=before_typed,
                after=None,
            )
        except Exception as exc:
            raise PgoutputError(f"Error creating ChangeEvent: {exc}")

    def process_truncate(
            self, message: ReplicationMessage, transaction: Transaction
    ) -> typing.Generator[ChangeEvent, None, None]:
        decoded_msg: Truncate = Truncate(message.payload)
        for relation_id in decoded_msg.relation_ids:
            try:
                yield ChangeEvent(
                    op=decoded_msg.byte1,
                    message_id=message.message_id,
                    lsn=message.data_start,
                    transaction=transaction,
                    table_schema=self.table_schemas[relation_id],
                    before=None,
                    after=None,
                )
            except Exception as exc:
                raise PgoutputError(f"Error creating ChangeEvent: {exc}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', dest='host', type=str, default='127.0.0.1')
    parser.add_argument('--port', dest='port', type=int, default=5433)
    parser.add_argument('--database', dest='database', type=str, default='postgres')
    parser.add_argument('--user', dest='user', type=str, default='postgres')
    parser.add_argument('--password', dest='password', type=str, default='postgres')
    parser.add_argument('--slot', dest='slot', type=str, default='test_slot')
    parser.add_argument('--send-feedback', dest='send_feedback', action='store_true')
    parser.add_argument('--max', dest='max_num', type=int, default=None, help='max number of messages to consume before exiting')
    parser.add_argument('-l', '--level', dest='level', type=str, default='info', help='logging level')
    parser.add_argument('--auto-create', dest='auto_create', action='store_true', help='auto create slot', default=False)

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.level.upper()),
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        datefmt='%Y:%m:%d %H:%M:%S.%f'
    )

    max_num = args.max_num

    if max_num is not None:
        num_consumed = 0

        def increase_num():
            nonlocal num_consumed
            num_consumed += 1
            return num_consumed == max_num

    else:
        def increase_num():
            return False

    def process_result(result):
        result = asdict(result)
        print(json.dumps(result, indent=4, default=str))
        return increase_num()

    reader = LogicalReplicationReader(
        publication_name=args.slot,
        slot_name=args.slot,
        host=args.host,
        database=args.database,
        port=args.port,
        user=args.user,
        password=args.password,
        send_feedback=args.send_feedback,
        process_result=process_result,
        auto_create=args.auto_create,
    )
    reader.start()


if __name__ == '__main__':
    main()
