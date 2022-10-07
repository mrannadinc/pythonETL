from tableauhyperapi import HyperProcess, Connection, Telemetry, CreateMode

# Start hyper process
with HyperProcess (telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    # Create a connection
    with Connection (endpoint=hyper.endpoint, database='databases/hello-world.hyper', create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
        # Print something
        print()
        connection.execute_scalar_query(query="select 'Hello World'")
