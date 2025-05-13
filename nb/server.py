import station_pb2_grpc
import station_pb2
import grpc
from concurrent import futures
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
import cassandra
import traceback


class station_record(object):

    def __init__(self, tmin, tmax):
        self.tmin = tmin
        self.tmax = tmax

class StationService(station_pb2_grpc.StationServicer):
    def __init__(self):
        #Connect to the cass clusters
        cluster = Cluster(['p6-db-1', 'p6-db-2', 'p6-db-3'])
        self.cass = cluster.connect()

        cluster.register_user_type('weather', 'station_record', station_record)
#(keyspace name, cassandra obj name, python obj name form top)

    def RecordTemps(self, request, context):
        try:
            insert_statement = self.cass.prepare("""
                INSERT INTO weather.stations (id, date, record) 
                VALUES (?, ?, ?)         
            """)
            insert_statement.consistency_level = ConsistencyLevel.ONE
    
            self.cass.execute(insert_statement, (request.station, request.date, station_record(request.tmin , request.tmax)) )
            
            return station_pb2.RecordTempsReply(error = "")
        except Exception:
            err = traceback.format_exc()
            return station_pb2.RecordTempsReply(error =err)

    def StationMax(self, request, context):
        try:
            max_statement = self.cass.prepare("""
                SELECT MAX(record.tmax) 
                FROM weather.stations 
                WHERE id = ?            
            """)
            max_statement.consistency_level = ConsistencyLevel.THREE
            result = self.cass.execute(max_statement, (request.station,))
            tmax = result.one()[0]
            return station_pb2.StationMaxReply(tmax = tmax, error = "")
            
        except cassandra.Unavailable as e:
            err = f"need {e.required_replicas} replicas, but only have {e.alive_replicas}"
            return station_pb2.StationMaxReply(tmax = -1, error = err)

        except cassandra.cluster.NoHostAvailable as e:
            for error in e.errors:
                err = "Error no Host Available"
                if type(e.errors[error]) == type(cassandra.Unavailable):
                    err = 'need ' + str(e.required_replicas) +' replicas, but only have '+ str(e.alive_replicas)
                    break
                else:
                    err = "Error No Host Available"  #traceback.format_exc()
            return station_pb2.StationMaxReply(tmax = -1, error = err)
        
        except Exception:
            err = "Other Error" #traceback.format_exc()
            return station_pb2.StationMaxReply(tmax = -1, error = err)

if __name__ == "__main__":
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=(('grpc.so_reuseport', 0),))
  station_pb2_grpc.add_StationServicer_to_server(StationService(),server)
  server.add_insecure_port("[::]:5440", )
  server.start()
  server.wait_for_termination()
