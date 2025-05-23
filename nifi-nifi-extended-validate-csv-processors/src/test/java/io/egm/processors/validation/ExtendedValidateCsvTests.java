/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.egm.processors.validation;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class ExtendedValidateCsvTests {

    @Test
    public void testHeaderAndSplit() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtendedValidateCsv());
        runner.setProperty(ExtendedValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ExtendedValidateCsv.END_OF_LINE_CHARACTER, "\n");
        runner.setProperty(ExtendedValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ExtendedValidateCsv.HEADER, "true");
        runner.setProperty(ExtendedValidateCsv.VALIDATION_STRATEGY, ExtendedValidateCsv.VALIDATE_LINES_INDIVIDUALLY);

        runner.setProperty(ExtendedValidateCsv.SCHEMA, "Null, ParseDate(\"dd/MM/yyyy\"), Optional(ParseDouble())");

        runner.enqueue("Name,Birthdate,Weight\nJohn,22/11/1954,63.2\nBob,01/03/2004,45.0");
        runner.run();

        runner.assertTransferCount(ExtendedValidateCsv.REL_VALID, 1);
        runner.getFlowFilesForRelationship(ExtendedValidateCsv.REL_VALID).get(0).assertContentEquals("Name,Birthdate,Weight\nJohn,22/11/1954,63.2\nBob,01/03/2004,45.0");
        runner.assertTransferCount(ExtendedValidateCsv.REL_INVALID, 0);

        runner.clearTransferState();

        runner.enqueue("Name,Birthdate,Weight\nJohn,22/11/1954,63a2\nBob,01/032004,45.0");
        runner.run();

        runner.assertTransferCount(ExtendedValidateCsv.REL_VALID, 0);
        runner.assertTransferCount(ExtendedValidateCsv.REL_INVALID, 1);
        runner.getFlowFilesForRelationship(ExtendedValidateCsv.REL_INVALID).get(0).assertContentEquals("Name,Birthdate,Weight\nJohn,22/11/1954,63a2\nBob,01/032004,45.0");

        runner.clearTransferState();

        runner.enqueue("Name,Birthdate,Weight\nJohn,22/111954,63.2\nBob,01/03/2004,45.0");
        runner.run();

        runner.assertTransferCount(ExtendedValidateCsv.REL_VALID, 1);
        runner.getFlowFilesForRelationship(ExtendedValidateCsv.REL_VALID).get(0).assertContentEquals("Name,Birthdate,Weight\nBob,01/03/2004,45.0");
        runner.assertTransferCount(ExtendedValidateCsv.REL_INVALID, 1);
        runner.getFlowFilesForRelationship(ExtendedValidateCsv.REL_INVALID).get(0).assertContentEquals("Name,Birthdate,Weight\nJohn,22/111954,63.2");
    }

    @Test
    public void testNullValues() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtendedValidateCsv());
        runner.setProperty(ExtendedValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ExtendedValidateCsv.END_OF_LINE_CHARACTER, "\n");
        runner.setProperty(ExtendedValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ExtendedValidateCsv.HEADER, "true");
        runner.setProperty(ExtendedValidateCsv.VALIDATION_STRATEGY, ExtendedValidateCsv.VALIDATE_LINES_INDIVIDUALLY);

        runner.setProperty(ExtendedValidateCsv.SCHEMA, "Null, Null, Null");

        runner.enqueue("#Name,Birthdate,Weight\nJohn,\"\",63.2\nBob,,45.0");
        runner.run();

        runner.assertTransferCount(ExtendedValidateCsv.REL_VALID, 1);
        runner.getFlowFilesForRelationship(ExtendedValidateCsv.REL_VALID).get(0).assertContentEquals("#Name,Birthdate,Weight\nJohn,\"\",63.2\nBob,,45.0");
        runner.assertTransferCount(ExtendedValidateCsv.REL_INVALID, 0);
    }

    @Test
    public void testUniqueWithSplit() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtendedValidateCsv());
        runner.setProperty(ExtendedValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ExtendedValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ExtendedValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ExtendedValidateCsv.HEADER, "false");
        runner.setProperty(ExtendedValidateCsv.VALIDATION_STRATEGY, ExtendedValidateCsv.VALIDATE_LINES_INDIVIDUALLY);

        runner.setProperty(ExtendedValidateCsv.SCHEMA, "Unique()");

        runner.enqueue("John\r\nBob\r\nBob\r\nJohn\r\nTom");
        runner.run();

        runner.assertTransferCount(ExtendedValidateCsv.REL_VALID, 1);
        runner.assertTransferCount(ExtendedValidateCsv.REL_INVALID, 1);

        runner.getFlowFilesForRelationship(ExtendedValidateCsv.REL_VALID).get(0).assertContentEquals("John\r\nBob\r\nTom");
        runner.getFlowFilesForRelationship(ExtendedValidateCsv.REL_VALID).get(0).assertAttributeEquals("count.total.lines", "5");
        runner.getFlowFilesForRelationship(ExtendedValidateCsv.REL_VALID).get(0).assertAttributeEquals("count.valid.lines", "3");
        runner.getFlowFilesForRelationship(ExtendedValidateCsv.REL_INVALID).get(0).assertContentEquals("Bob\r\nJohn");
        runner.getFlowFilesForRelationship(ExtendedValidateCsv.REL_INVALID).get(0).assertAttributeEquals("count.invalid.lines", "2");
        runner.getFlowFilesForRelationship(ExtendedValidateCsv.REL_INVALID).get(0).assertAttributeEquals("count.total.lines", "5");
    }

    @Test
    public void testValidDateOptionalDouble() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtendedValidateCsv());
        runner.setProperty(ExtendedValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ExtendedValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ExtendedValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ExtendedValidateCsv.HEADER, "false");

        runner.setProperty(ExtendedValidateCsv.INCLUDE_ALL_VIOLATIONS, "true");


        runner.setProperty(ExtendedValidateCsv.SCHEMA, "Null, ParseDate(\"dd/MM/yyyy\"), Optional(ParseDouble())");

        runner.enqueue("John,22/11/1954,63.2\r\nBob,01/03/2004,45.0");
        runner.run();
        runner.assertAllFlowFilesTransferred(ExtendedValidateCsv.REL_VALID, 1);

        runner.enqueue("John,22/111954,abc\r\nBob,01/03/2004,45.0");
        runner.run();
        runner.assertTransferCount(ExtendedValidateCsv.REL_INVALID, 1);
        runner.getFlowFilesForRelationship(ExtendedValidateCsv.REL_INVALID).get(0).assertAttributeEquals("validation.error.message",
                "\n\t\tline 1 : '22/111954' could not be parsed as a Date at column 2, 'abc' could not be parsed as a Double at column 3");
    }

    @Test
    public void testIsIncludedIn() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtendedValidateCsv());
        runner.setProperty(ExtendedValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ExtendedValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ExtendedValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ExtendedValidateCsv.HEADER, "false");

        runner.setProperty(ExtendedValidateCsv.SCHEMA, "Null, ParseDate(\"dd/MM/yyyy\"), IsIncludedIn(\"male\", \"female\")");

        runner.enqueue("John,22/11/1954,male\r\nMarie,01/03/2004,female");
        runner.run();
        runner.assertAllFlowFilesTransferred(ExtendedValidateCsv.REL_VALID, 1);

        runner.enqueue("John,22/111954,63.2\r\nBob,01/03/2004,45.0");
        runner.run();
        runner.assertTransferCount(ExtendedValidateCsv.REL_INVALID, 1);
    }

    @Test
    public void testBigDecimalBoolCharIntLong() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtendedValidateCsv());
        runner.setProperty(ExtendedValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ExtendedValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ExtendedValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ExtendedValidateCsv.HEADER, "true");

        runner.setProperty(ExtendedValidateCsv.SCHEMA, "ParseBigDecimal(), ParseBool(), ParseChar(), ParseInt(), ParseLong()");

        runner.enqueue("bigdecimal,bool,char,integer,long\r\n10.0001,true,c,1,92147483647");
        runner.run();
        runner.assertAllFlowFilesTransferred(ExtendedValidateCsv.REL_VALID, 1);

        runner.enqueue("bigdecimal,bool,char,integer,long\r\n10.0001,true,c,92147483647,92147483647");
        runner.run();
        runner.assertTransferCount(ExtendedValidateCsv.REL_INVALID, 1);
    }

    @Test
    public void testEqualsNotNullStrNotNullOrEmpty() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtendedValidateCsv());
        runner.setProperty(ExtendedValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ExtendedValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ExtendedValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ExtendedValidateCsv.HEADER, "false");

        runner.setProperty(ExtendedValidateCsv.SCHEMA, "Equals(), NotNull(), StrNotNullOrEmpty()");

        runner.enqueue("test,test,test\r\ntest,test,test");
        runner.run();
        runner.assertAllFlowFilesTransferred(ExtendedValidateCsv.REL_VALID, 1);

        runner.enqueue("test,test,test\r\ntset,test,test");
        runner.run();
        runner.assertTransferCount(ExtendedValidateCsv.REL_INVALID, 1);
    }

    @Test
    public void testStrlenStrMinMaxStrRegex() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtendedValidateCsv());
        runner.setProperty(ExtendedValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ExtendedValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ExtendedValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ExtendedValidateCsv.HEADER, "false");

        runner.setProperty(ExtendedValidateCsv.SCHEMA, "Strlen(4), StrMinMax(3,5), StrRegex(\"[a-z0-9\\._]+@[a-z0-9\\.]+\")");

        runner.enqueue("test,test,test@apache.org");
        runner.run();
        runner.assertAllFlowFilesTransferred(ExtendedValidateCsv.REL_VALID, 1);

        runner.enqueue("test,test,testapache.org");
        runner.run();
        runner.assertTransferCount(ExtendedValidateCsv.REL_INVALID, 1);
        runner.getFlowFilesForRelationship(ExtendedValidateCsv.REL_INVALID).get(0).assertAttributeEquals("validation.error.message",
                "\n\t\tline 1 : 'testapache.org' does not match the regular expression '[a-z0-9\\._]+@[a-z0-9\\.]+' at column 3");
    }

    @Test
    public void testDMinMaxForbidSubStrLMinMax() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtendedValidateCsv());
        runner.setProperty(ExtendedValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ExtendedValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ExtendedValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ExtendedValidateCsv.HEADER, "false");

        runner.setProperty(ExtendedValidateCsv.SCHEMA, "DMinMax(10,100),LMinMax(10,100),ForbidSubStr(\"test\", \"tset\")");

        runner.enqueue("50.001,50,hello");
        runner.run();
        runner.assertAllFlowFilesTransferred(ExtendedValidateCsv.REL_VALID, 1);

        runner.enqueue("10,10,testapache.org");
        runner.run();
        runner.assertTransferCount(ExtendedValidateCsv.REL_INVALID, 1);
    }

    @Test
    public void testUnique() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtendedValidateCsv());
        runner.setProperty(ExtendedValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ExtendedValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ExtendedValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ExtendedValidateCsv.HEADER, "false");

        runner.setProperty(ExtendedValidateCsv.SCHEMA, "Unique(), UniqueHashCode()");

        runner.enqueue("1,2\r\n3,4");
        runner.run();
        runner.assertAllFlowFilesTransferred(ExtendedValidateCsv.REL_VALID, 1);

        runner.enqueue("1,2\r\n1,4");
        runner.run();
        runner.assertTransferCount(ExtendedValidateCsv.REL_INVALID, 1);
    }

    @Test
    public void testRequire() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtendedValidateCsv());
        runner.setProperty(ExtendedValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ExtendedValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ExtendedValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ExtendedValidateCsv.HEADER, "false");

        int hashcode = "test".hashCode();
        runner.setProperty(ExtendedValidateCsv.SCHEMA, "RequireHashCode(" + hashcode + "), RequireSubStr(\"test\")");

        runner.enqueue("test,test");
        runner.run();
        runner.assertAllFlowFilesTransferred(ExtendedValidateCsv.REL_VALID, 1);

        runner.enqueue("tset,tset");
        runner.run();
        runner.assertTransferCount(ExtendedValidateCsv.REL_INVALID, 1);
    }

    @Test
    public void testValidate() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtendedValidateCsv());
        runner.setProperty(ExtendedValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ExtendedValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ExtendedValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ExtendedValidateCsv.HEADER, "false");
        runner.assertNotValid();

        // We
        runner.setProperty(ExtendedValidateCsv.SCHEMA, "RequireSubString(\"test\")");
        runner.assertNotValid();

        runner.setProperty(ExtendedValidateCsv.SCHEMA, "''");
        runner.assertNotValid();

        runner.setProperty(ExtendedValidateCsv.SCHEMA, "\"\"");
        runner.assertNotValid();
    }

    @Test
    public void testValidateWithEL() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtendedValidateCsv());
        runner.setProperty(ExtendedValidateCsv.DELIMITER_CHARACTER, "${comma}");
        runner.setProperty(ExtendedValidateCsv.END_OF_LINE_CHARACTER, "${crlf}");
        runner.setProperty(ExtendedValidateCsv.QUOTE_CHARACTER, "${quote}");
        runner.setProperty(ExtendedValidateCsv.HEADER, "false");

        runner.setProperty(ExtendedValidateCsv.SCHEMA, "RequireSubString(\"test\")");
        runner.assertNotValid();

        runner.setProperty(ExtendedValidateCsv.SCHEMA, "''");
        runner.assertNotValid();

        runner.setProperty(ExtendedValidateCsv.SCHEMA, "\"\"");
        runner.assertNotValid();

        runner.setProperty(ExtendedValidateCsv.SCHEMA, "${schema}");
        runner.assertValid();

        int hashcode = "test".hashCode();
        runner.enqueue(
            "test,test",
            Map.ofEntries(
                Map.entry("schema", "RequireHashCode(" + hashcode + "), RequireSubStr(\"test\")"),
                Map.entry("comma", ","),
                Map.entry("quote", "\""),
                Map.entry("crlf", "\r\n")
            )
        );
        runner.run();
        runner.assertAllFlowFilesTransferred(ExtendedValidateCsv.REL_VALID, 1);
    }

    @Test
    public void testParseSchemaCommaBoundary() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtendedValidateCsv());
        runner.setProperty(ExtendedValidateCsv.SCHEMA, "Null(),Null");
        runner.assertValid();
    }

    @Test
    public void testMultipleRuns() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtendedValidateCsv());
        runner.setProperty(ExtendedValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ExtendedValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ExtendedValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ExtendedValidateCsv.HEADER, "false");
        runner.setProperty(ExtendedValidateCsv.VALIDATION_STRATEGY, ExtendedValidateCsv.VALIDATE_LINES_INDIVIDUALLY);

        runner.setProperty(ExtendedValidateCsv.SCHEMA, "Unique()");

        runner.enqueue("John\r\nBob\r\nTom");
        runner.enqueue("John\r\nBob\r\nTom");
        runner.run(2);

        runner.assertTransferCount(ExtendedValidateCsv.REL_VALID, 2);
        runner.assertTransferCount(ExtendedValidateCsv.REL_INVALID, 0);
    }

    @Test
    public void testEscapingLineByLine() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtendedValidateCsv());
        runner.setProperty(ExtendedValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ExtendedValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ExtendedValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ExtendedValidateCsv.HEADER, "true");
        runner.setProperty(ExtendedValidateCsv.VALIDATION_STRATEGY, ExtendedValidateCsv.VALIDATE_LINES_INDIVIDUALLY);

        final String row = "Header1,\"Header2,escaped\",Header3\r\nField1,\"Field2,escaped\",Field3";
        runner.setProperty(ExtendedValidateCsv.SCHEMA, "ParseInt(),ParseInt(),ParseInt()");

        runner.enqueue(row);
        runner.run(1);

        runner.assertTransferCount(ExtendedValidateCsv.REL_VALID, 0);
        runner.assertTransferCount(ExtendedValidateCsv.REL_INVALID, 1);
        runner.getFlowFilesForRelationship(ExtendedValidateCsv.REL_INVALID).get(0).assertContentEquals(row);
        runner.clearTransferState();

        runner.setProperty(ExtendedValidateCsv.SCHEMA, "null,null,null");
        runner.enqueue(row);
        runner.run(1);

        runner.assertTransferCount(ExtendedValidateCsv.REL_VALID, 1);
        runner.assertTransferCount(ExtendedValidateCsv.REL_INVALID, 0);
        runner.getFlowFilesForRelationship(ExtendedValidateCsv.REL_VALID).get(0).assertContentEquals(row);
    }

    @Test
    public void testQuote() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtendedValidateCsv());
        runner.setProperty(ExtendedValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ExtendedValidateCsv.END_OF_LINE_CHARACTER, "\n");
        runner.setProperty(ExtendedValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ExtendedValidateCsv.HEADER, "true");
        runner.setProperty(ExtendedValidateCsv.VALIDATION_STRATEGY, ExtendedValidateCsv.VALIDATE_LINES_INDIVIDUALLY);

        runner.setProperty(ExtendedValidateCsv.SCHEMA, "NotNull(), NotNull(), NotNull()");

        runner.enqueue("Header 1, Header 2, Header 3\n\"Content 1a, Content 1b\", Content 2, Content 3");
        runner.run();

        runner.assertTransferCount(ExtendedValidateCsv.REL_VALID, 1);
        runner.getFlowFilesForRelationship(ExtendedValidateCsv.REL_VALID).get(0).assertContentEquals("Header 1, Header 2, Header 3\n\"Content 1a, Content 1b\", Content 2, Content 3");
        runner.assertTransferCount(ExtendedValidateCsv.REL_INVALID, 0);
    }

    @Test
    public void testValidateLinesIndividuallyAndIncludeAllViolations() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtendedValidateCsv());
        runner.setProperty(ExtendedValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ExtendedValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ExtendedValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ExtendedValidateCsv.HEADER, "false");
        runner.setProperty(ExtendedValidateCsv.VALIDATION_STRATEGY, ExtendedValidateCsv.VALIDATE_LINES_INDIVIDUALLY);

        runner.setProperty(ExtendedValidateCsv.INCLUDE_ALL_VIOLATIONS, "true");


        runner.setProperty(ExtendedValidateCsv.SCHEMA, "Null, ParseDate(\"dd/MM/yyyy\"), Optional(ParseDouble())");

        runner.enqueue("John,22/111954,abc\r\nBob,01/03/2004,45.0\r\nMary,10/071998,38.5\r\nHarry,15/14367,34.7");
        runner.run();
        runner.assertTransferCount(ExtendedValidateCsv.REL_VALID, 1);
        runner.assertTransferCount(ExtendedValidateCsv.REL_INVALID, 1);

        runner.getFlowFilesForRelationship(ExtendedValidateCsv.REL_INVALID).get(0).assertAttributeEquals("validation.error.message",
                "\n\t\tline 1 : '22/111954' could not be parsed as a Date at column 2, 'abc' could not be parsed as a Double at column 3\n" +
                        "\t\tline 3 : '10/071998' could not be parsed as a Date at column 2\n" +
                        "\t\tline 4 : '15/14367' could not be parsed as a Date at column 2\n");
    }

    @Test
    public void testValidateLinesIndividuallyAndIncludeAllViolationsWithValidLinesOnly() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtendedValidateCsv());
        runner.setProperty(ExtendedValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ExtendedValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ExtendedValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ExtendedValidateCsv.HEADER, "false");
        runner.setProperty(ExtendedValidateCsv.VALIDATION_STRATEGY, ExtendedValidateCsv.VALIDATE_LINES_INDIVIDUALLY);

        runner.setProperty(ExtendedValidateCsv.INCLUDE_ALL_VIOLATIONS, "true");


        runner.setProperty(ExtendedValidateCsv.SCHEMA, "Null, ParseDate(\"dd/MM/yyyy\"), Optional(ParseDouble())");

        runner.enqueue("John,22/11/1954,45.8\r\nBob,01/03/2004,45.0\r\nMary,10/07/1998,38.5");
        runner.run();
        runner.assertTransferCount(ExtendedValidateCsv.REL_VALID, 1);
        runner.assertTransferCount(ExtendedValidateCsv.REL_INVALID, 0);

    }

    @Test
    public void testValidateWholeFlowFileAndAccumulateAllErrors() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtendedValidateCsv());
        runner.setProperty(ExtendedValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ExtendedValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ExtendedValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ExtendedValidateCsv.HEADER, "false");
        runner.setProperty(ExtendedValidateCsv.VALIDATION_STRATEGY, ExtendedValidateCsv.VALIDATE_WHOLE_FLOWFILE);

        runner.setProperty(ExtendedValidateCsv.ACCUMULATE_ALL_ERRORS, "true");


        runner.setProperty(ExtendedValidateCsv.SCHEMA, "Null, ParseDate(\"dd/MM/yyyy\"), Optional(ParseDouble())");

        runner.enqueue("John,22/111954,abc\r\nBob,01/03/2004,45.0\r\nMary,10/071998,38.5");
        runner.run();
        runner.assertTransferCount(ExtendedValidateCsv.REL_VALID, 0);
        runner.assertTransferCount(ExtendedValidateCsv.REL_INVALID, 1);

        runner.getFlowFilesForRelationship(ExtendedValidateCsv.REL_INVALID).get(0).assertAttributeEquals("validation.error.message",
                "\n\t\tline 1 : '22/111954' could not be parsed as a Date at column 2\n" +
                        "\t\tline 3 : '10/071998' could not be parsed as a Date at column 2\n");
    }

    @Test
    public void testValidateWholeFlowFileAndAccumulateAllErrorsWithValidLinesOnly() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtendedValidateCsv());
        runner.setProperty(ExtendedValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ExtendedValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ExtendedValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ExtendedValidateCsv.HEADER, "false");
        runner.setProperty(ExtendedValidateCsv.VALIDATION_STRATEGY, ExtendedValidateCsv.VALIDATE_WHOLE_FLOWFILE);

        runner.setProperty(ExtendedValidateCsv.ACCUMULATE_ALL_ERRORS, "true");


        runner.setProperty(ExtendedValidateCsv.SCHEMA, "Null, ParseDate(\"dd/MM/yyyy\"), Optional(ParseDouble())");

        runner.enqueue("John,22/11/1954,45.8\r\nBob,01/03/2004,45.0\r\nMary,10/07/1998,38.5");
        runner.run();
        runner.assertTransferCount(ExtendedValidateCsv.REL_VALID, 1);
        runner.assertTransferCount(ExtendedValidateCsv.REL_INVALID, 0);

    }

    @Test
    public void testValidateWholeFlowFileAccumulateAllErrorsAndIncludeAllViolations() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtendedValidateCsv());
        runner.setProperty(ExtendedValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ExtendedValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ExtendedValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ExtendedValidateCsv.HEADER, "false");

        runner.setProperty(ExtendedValidateCsv.ACCUMULATE_ALL_ERRORS, "true");
        runner.setProperty(ExtendedValidateCsv.INCLUDE_ALL_VIOLATIONS, "true");



        runner.setProperty(ExtendedValidateCsv.SCHEMA, "Null, ParseDate(\"dd/MM/yyyy\"), Optional(ParseDouble())");

        runner.enqueue("John,2211/1954,abc\r\nBob,01/03/2004,45.0\r\nMary,10/071998,abc\r\nHarry,01/05/1988,56.5");
        runner.run();
        runner.assertTransferCount(ExtendedValidateCsv.REL_INVALID, 1);
        runner.assertTransferCount(ExtendedValidateCsv.REL_VALID, 0);


        runner.getFlowFilesForRelationship(ExtendedValidateCsv.REL_INVALID).get(0).assertAttributeEquals("validation.error.message",
                "\n\t\tline 1 : '2211/1954' could not be parsed as a Date at column 2, 'abc' could not be parsed as a Double at column 3\n" +
                        "\t\tline 3 : '10/071998' could not be parsed as a Date at column 2, 'abc' could not be parsed as a Double at column 3\n");

    }

    @Test
    public void testValidateWholeFlowFileWithoutAccumulateAllErrorsAndWithoutIncludeAllViolations() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtendedValidateCsv());
        runner.setProperty(ExtendedValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ExtendedValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ExtendedValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ExtendedValidateCsv.HEADER, "false");


        runner.setProperty(ExtendedValidateCsv.SCHEMA, "Null, ParseDate(\"dd/MM/yyyy\"), Optional(ParseDouble())");

        runner.enqueue("John,2211/1954,abc\r\nBob,01/03/2004,45.0\r\nMary,10/071998,abc");
        runner.run();
        runner.assertTransferCount(ExtendedValidateCsv.REL_INVALID, 1);
        runner.assertTransferCount(ExtendedValidateCsv.REL_VALID, 0);


        runner.getFlowFilesForRelationship(ExtendedValidateCsv.REL_INVALID).get(0).assertAttributeEquals("validation.error.message",
                "\n\t\tline 1 : '2211/1954' could not be parsed as a Date at column 2");

    }
}
