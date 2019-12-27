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

import org.apache.spark.sql.catalyst.CatalystTypeConverters.CatalystTypeConverter
import org.apache.spark.sql.catalyst.expressions.Attribute

object GenerateStructConverter
  // extends CodeGenerator[Class[_], CatalystTypeConverter[Any, Any, Any]] {
  extends CodeGenerator[Class[_], Any => Any] {

  protected def bind(in: Class[_], inputSchema: Seq[Attribute]): Class[_] = in
  //  BindReferences.bindReference(in, inputSchema)

  protected def canonicalize(in: Class[_]): Class[_] = in // ExpressionCanonicalizer.execute(in)

  protected def create(in: Class[_]): Any => Any = {
    val ctx = newCodeGenContext()
//    val eval = in.genCode(ctx)

//    val code = CodeFormatter.stripOverlappingComments(
//      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
//    logDebug(s"Generated predicate '$predicate':\n${CodeFormatter.format(code)}")

    // scalastyle:off
    val codeBody =
      s"""
        |public java.lang.Object generate(Object[] references) {
        |  return new SpecificStructConverter();
        |}
        |
        |public static class SpecificStructConverter extends ${classOf[CatalystTypeConverter[_, _, _]].getName} {
        |  public Object toScala(Object in) {
        |    InternalRow row = (InternalRow)in;
        |    return "Hello";
        |  }
        |
        |  InternalRow toCatalystImpl(Object scalaValue) {
        |    throw new UnsupportedOperationException();
        |  }
        |
        |  Object toScalaImpl(InternalRow in, int column) {
        |    InternalRow row = (InternalRow)in;
        |    throw new UnsupportedOperationException();
        |  }
        |  public Object apply(InternalRow row) {
        |    return "Hello";
        |  }
        |
        |//  public Object apply(final Object obj) {
        |//    return apply((InternalRow)obj);
        |//  }
        |}
        |""".stripMargin
    // scalastyle:on
    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    val (clazz, _) = CodeGenerator.compile(code)
    val f = clazz.generate(null).asInstanceOf[CatalystTypeConverter[Any, Any, Any]]
    f.getClass.getDeclaredMethods.foreach(println)
    // clazz.generate(null).asInstanceOf
    // new ConverterWrapper(f).toScala
    // val x = 10
    // ((s: String) => s * x).asInstanceOf[Any => Any]
    // ((s: String) => s.toString).asInstanceOf[Any => Any]
    // (new Fuga.Hoge().func _).asInstanceOf[Any => Any]
    // ((s: Double) => java.lang.Math.abs(s)).asInstanceOf[Any => Any]
    import java.io._
    val output = new ObjectOutputStream(new FileOutputStream("/tmp/test.obj"))
    output.writeObject(f)
    output.close()

    val input = new ObjectInputStream(new FileInputStream("/tmp/test.obj"))
    input.readObject()
    input.close()

     val piyo = new Piyo
     val fuga = new piyo.Fuga
     val hoge = new fuga.Hoge
     (hoge.func _).asInstanceOf[Any => Any]
  }
}

class Piyo extends Serializable {

  class Fuga extends Serializable {

    class Hoge extends Serializable {
      def func(s: String): String = s.toString
    }
  }
}
