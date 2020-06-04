#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pyspark.broadcast import Broadcast
# from pyspark.rdd import PythonEvalType
#from pyspark.serializers import write_with_length, write_int, read_long, read_bool, \
#    write_long, read_int, SpecialLengths, UTF8Deserializer, PickleSerializer, \
#    BatchedSerializer
from pyspark.serializers import PickleSerializer

pickleSer = PickleSerializer()

print("Loading Graal Runner")

def read_command(serializer, obj):
#    command = serializer._read_with_length(file)
    command = serializer.loads(obj)
    if isinstance(command, Broadcast):
        command = serializer.loads(command.value)
    return command


def test_udf(obj):
    f, return_type = read_command(pickleSer, bytes(obj))
    return f
    
