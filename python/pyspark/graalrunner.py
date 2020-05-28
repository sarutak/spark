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

from pyspark.rdd import PythonEvalType
from pyspark.serializers import write_with_length, write_int, read_long, read_bool, \
    write_long, read_int, SpecialLengths, UTF8Deserializer, PickleSerializer, \
    BatchedSerializer

pickleSer = PickleSerializer()


def deserialize_single_udf(serialized_udf):

def deserialize_udfs(serialized_udfs, num_udfs):
    

def read_single_udf(pickleSer, num_arg, eval_type, udf_index):
#    num_arg = read_int(infile)
    arg_offsets = [read_int(infile) for i in range(num_arg)]
    chained_func = None
    for i in range(read_int(infile)):
        f, return_type = read_command(pickleSer, infile)
        if chained_func is None:
            chained_func = f
        else:
            chained_func = chain(chained_func, f)

    if eval_type == PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF:
        func = chained_func
    else:
        # make sure StopIteration's raised in the user code are not ignored
        # when they are processed in a for loop, raise them as RuntimeError's instead
        func = fail_on_stopiteration(chained_func)

    # the last returnType will be the return type of UDF
    if eval_type == PythonEvalType.SQL_SCALAR_PANDAS_UDF:
        return arg_offsets, wrap_scalar_pandas_udf(func, return_type)
    elif eval_type == PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF:
        return arg_offsets, wrap_pandas_iter_udf(func, return_type)
    elif eval_type == PythonEvalType.SQL_MAP_PANDAS_ITER_UDF:
        return arg_offsets, wrap_pandas_iter_udf(func, return_type)
    elif eval_type == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF:
        argspec = _get_argspec(chained_func)  # signature was lost when wrapping it
        return arg_offsets, wrap_grouped_map_pandas_udf(func, return_type, argspec)
    elif eval_type == PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF:
        argspec = _get_argspec(chained_func)  # signature was lost when wrapping it
        return arg_offsets, wrap_cogrouped_map_pandas_udf(func, return_type, argspec)
    elif eval_type == PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF:
        return arg_offsets, wrap_grouped_agg_pandas_udf(func, return_type)
    elif eval_type == PythonEvalType.SQL_WINDOW_AGG_PANDAS_UDF:
        return arg_offsets, wrap_window_agg_pandas_udf(func, return_type, runner_conf, udf_index)
    elif eval_type == PythonEvalType.SQL_BATCHED_UDF:
        return arg_offsets, wrap_udf(func, return_type)
    else:
        raise ValueError("Unknown eval type: {}".format(eval_type))

def read_udfs(pickleSer, num_udfs, eval_type):
#    runner_conf = {}

    if eval_type in (PythonEvalType.SQL_SCALAR_PANDAS_UDF,
                     PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF,
                     PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF,
                     PythonEvalType.SQL_MAP_PANDAS_ITER_UDF,
                     PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
                     PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF,
                     PythonEvalType.SQL_WINDOW_AGG_PANDAS_UDF):
        pass

        # Load conf used for pandas_udf evaluation
#        num_conf = read_int(infile)
#        for i in range(num_conf):
#            k = utf8_deserializer.loads(infile)
#            v = utf8_deserializer.loads(infile)
#            runner_conf[k] = v

        # NOTE: if timezone is set here, that implies respectSessionTimeZone is True
#        timezone = runner_conf.get("spark.sql.session.timeZone", None)
#        safecheck = runner_conf.get("spark.sql.execution.pandas.convertToArrowArraySafely",
#                                    "false").lower() == 'true'
        # Used by SQL_GROUPED_MAP_PANDAS_UDF and SQL_SCALAR_PANDAS_UDF when returning StructType
#        assign_cols_by_name = runner_conf.get(
#            "spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName", "true")\
#            .lower() == "true"

#        if eval_type == PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF:
#            ser = CogroupUDFSerializer(timezone, safecheck, assign_cols_by_name)
#        else:
            # Scalar Pandas UDF handles struct type arguments as pandas DataFrames instead of
            # pandas Series. See SPARK-27240.
#            df_for_struct = (eval_type == PythonEvalType.SQL_SCALAR_PANDAS_UDF or
#                             eval_type == PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF or
#                             eval_type == PythonEvalType.SQL_MAP_PANDAS_ITER_UDF)
#            ser = ArrowStreamPandasUDFSerializer(timezone, safecheck, assign_cols_by_name,
                                                 df_for_struct)
    else:
        ser = BatchedSerializer(PickleSerializer(), 100)

#    num_udfs = read_int(infile)

    is_scalar_iter = eval_type == PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF
    is_map_iter = eval_type == PythonEvalType.SQL_MAP_PANDAS_ITER_UDF

    if is_scalar_iter or is_map_iter:
        pass
#        if is_scalar_iter:
#            assert num_udfs == 1, "One SCALAR_ITER UDF expected here."
#        if is_map_iter:
#            assert num_udfs == 1, "One MAP_ITER UDF expected here."
#
#        arg_offsets, udf = read_single_udf(
#            pickleSer, infile, eval_type, runner_conf, udf_index=0)
#
#        def func(_, iterator):
#            num_input_rows = [0]  # TODO(SPARK-29909): Use nonlocal after we drop Python 2.
#
#            def map_batch(batch):
#                udf_args = [batch[offset] for offset in arg_offsets]
#                num_input_rows[0] += len(udf_args[0])
#                if len(udf_args) == 1:
#                    return udf_args[0]
#                else:
#                    return tuple(udf_args)
#
#            iterator = map(map_batch, iterator)
#            result_iter = udf(iterator)
#
#            num_output_rows = 0
#            for result_batch, result_type in result_iter:
#                num_output_rows += len(result_batch)
#                # This assert is for Scalar Iterator UDF to fail fast.
#                # The length of the entire input can only be explicitly known
#                # by consuming the input iterator in user side. Therefore,
#                # it's very unlikely the output length is higher than
#                # input length.
#                assert is_map_iter or num_output_rows <= num_input_rows[0], \
#                    "Pandas SCALAR_ITER UDF outputted more rows than input rows."
#                yield (result_batch, result_type)
#
#            if is_scalar_iter:
#                try:
#                    next(iterator)
#                except StopIteration:
#                    pass
#                else:
#                    raise RuntimeError("pandas iterator UDF should exhaust the input "
#                                       "iterator.")
#
#                if num_output_rows != num_input_rows[0]:
#                    raise RuntimeError(
#                        "The length of output in Scalar iterator pandas UDF should be "
#                        "the same with the input's; however, the length of output was %d and the "
#                        "length of input was %d." % (num_output_rows, num_input_rows[0]))
#
#        # profiling is not supported for UDF
#        return func, None, ser, ser

#    def extract_key_value_indexes(grouped_arg_offsets):
#        """
#        Helper function to extract the key and value indexes from arg_offsets for the grouped and
#        cogrouped pandas udfs. See BasePandasGroupExec.resolveArgOffsets for equivalent scala code.
#
#        :param grouped_arg_offsets:  List containing the key and value indexes of columns of the
#            DataFrames to be passed to the udf. It consists of n repeating groups where n is the
#            number of DataFrames.  Each group has the following format:
#                group[0]: length of group
#                group[1]: length of key indexes
#                group[2.. group[1] +2]: key attributes
#                group[group[1] +3 group[0]]: value attributes
#        """
#        parsed = []
#        idx = 0
#        while idx < len(grouped_arg_offsets):
#            offsets_len = grouped_arg_offsets[idx]
#            idx += 1
#            offsets = grouped_arg_offsets[idx: idx + offsets_len]
#            split_index = offsets[0] + 1
#            offset_keys = offsets[1: split_index]
#            offset_values = offsets[split_index:]
#            parsed.append([offset_keys, offset_values])
#            idx += offsets_len
#        return parsed

    if eval_type == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF:
        pass
#        # We assume there is only one UDF here because grouped map doesn't
#        # support combining multiple UDFs.
#        assert num_udfs == 1
#
#        # See FlatMapGroupsInPandasExec for how arg_offsets are used to
#        # distinguish between grouping attributes and data attributes
#        arg_offsets, f = read_single_udf(pickleSer, infile, eval_type, runner_conf, udf_index=0)
#        parsed_offsets = extract_key_value_indexes(arg_offsets)
#
#        # Create function like this:
#        #   mapper a: f([a[0]], [a[0], a[1]])
#        def mapper(a):
#            keys = [a[o] for o in parsed_offsets[0][0]]
#            vals = [a[o] for o in parsed_offsets[0][1]]
#            return f(keys, vals)
    elif eval_type == PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF:
        pass
#        # We assume there is only one UDF here because cogrouped map doesn't
#        # support combining multiple UDFs.
#        assert num_udfs == 1
#        arg_offsets, f = read_single_udf(pickleSer, infile, eval_type, runner_conf, udf_index=0)
#
#        parsed_offsets = extract_key_value_indexes(arg_offsets)
#
#        def mapper(a):
#            df1_keys = [a[0][o] for o in parsed_offsets[0][0]]
#            df1_vals = [a[0][o] for o in parsed_offsets[0][1]]
#            df2_keys = [a[1][o] for o in parsed_offsets[1][0]]
#            df2_vals = [a[1][o] for o in parsed_offsets[1][1]]
#            return f(df1_keys, df1_vals, df2_keys, df2_vals)
    else:
        udfs = []
        for i in range(num_udfs):
            num_arg = 
            udfs.append(read_single_udf(pickleSer, eval_type, udf_index=i))

        def mapper(a):
            result = tuple(f(*[a[o] for o in arg_offsets]) for (arg_offsets, f) in udfs)
            # In the special case of a single UDF this will return a single result rather
            # than a tuple of results; this is the format that the JVM side expects.
            if len(result) == 1:
                return result[0]
            else:
                return result

    func = lambda _, it: map(mapper, it)

    # profiling is not supported for UDF
    return func, None, ser, ser


def execute_udf(func, iter):
    
func, profiler, deserializer, serializer = read_udfs(pickleSer, infile, eval_type)
