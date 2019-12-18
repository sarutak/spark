/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, StructConverter}

object GenerateStructConverter extends CodeGenerator[Class[_], StructConverter] {

  protected def bind(in: Class[_], inputSchema: Seq[Attribute]): Class[_] = in
  //  BindReferences.bindReference(in, inputSchema)

  protected def canonicalize(in: Class[_]): Class[_] = in // ExpressionCanonicalizer.execute(in)

  protected def create(in: Class[_]): StructConverter = {
    val ctx = newCodeGenContext()
//    val eval = in.genCode(ctx)

//    val code = CodeFormatter.stripOverlappingComments(
//      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
//    logDebug(s"Generated predicate '$predicate':\n${CodeFormatter.format(code)}")

    val codeBody =
      s"""
        |public java.lang.Object generate(Object[] references) {
        |  return new SpecificStructConverter();
        |}
        |
        |static class SpecificStructConverter extends ${classOf[StructConverter].getName} {
        |  public Object toScala(Object in) {
        |    InternalRow row = (InternalRow)in;
        |    return "Hello";
        |  }
        |}
        |""".stripMargin
    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(null).asInstanceOf[StructConverter]
  }
}
