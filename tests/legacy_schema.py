"""Build genuine pre-schema-5 fixtures without inventory columns or their index."""

def remove_metadata(connection):
    view=connection.execute("SELECT sql FROM sqlite_master WHERE name='device_overview'").fetchone()
    if view:connection.execute('DROP VIEW device_overview')
    connection.execute('DROP INDEX IF EXISTS devices_management_code')
    connection.execute('ALTER TABLE devices DROP COLUMN management_code')
    connection.execute('ALTER TABLE devices DROP COLUMN participant_code')
    if view:connection.execute(view[0].replace('d.management_code,d.participant_code,',''))
