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

import pandas as pd
import numpy as np
import pickle

pickleSer = pickle

# For debug
# print("Loading Graal Runner")

def read_command(serializer, obj):
    command = serializer.loads(obj)
    return command


def get_udf(obj):
    data = bytes(obj)
    f, return_type = read_command(pickleSer, data)
    return f


def get_pandas_scalar_udf(obj):
    f = get_udf(obj)

    def wrap_func(*args):
        new_arg_list = map(lambda arg: pd.Series(arg), args)
        result = f(*new_arg_list)
        return result

    return wrap_func

def get_pandas_grouped_agg_udf(obj):
    f = get_udf(obj)

    def wrap_func(*args):
        new_arg_list = map(lambda arg: pd.Series(arg), args)
        result = f(*new_arg_list)

        # The following code is necessary for float64
        # Because Value.fitsInFloat returns false with float 64.
        if result.dtype == np.dtype('float64'):
            return float(result)
        else:
            return result

    return wrap_func
