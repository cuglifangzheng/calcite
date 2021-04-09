/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.ConnectionSpec;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 *
 * @author lifangzheng
 * @version : MyTest.java, v 0.1 2021年03月23日 11:16 上午 lifangzheng Exp $
 */
public class MyTest {
  @Test
  void myTest() {
    ConnectionSpec SCOTT = CalciteAssert.DatabaseInstance.HSQLDB.scott;
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    Map<String, Object> map = new HashMap<>();
    map.put("jdbcDriver", SCOTT.driver);
    map.put("jdbcUser", SCOTT.username);
    map.put("jdbcPassword", SCOTT.password);
    map.put("jdbcUrl", SCOTT.url);
    map.put("jdbcCatalog", SCOTT.catalog);
    map.put("jdbcSchema", SCOTT.schema);
    JdbcSchema schema = JdbcSchema.create(rootSchema, "JDBC_SCOTT", map);
    rootSchema = rootSchema.add("  JDBC_SCOTT", schema);
    FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(rootSchema)
        .parserConfig(SqlParser.Config.DEFAULT)
        .programs(Programs.standard()).build();


    RelBuilder relBuilder = RelBuilder.create(config);
    RelNode relNode = relBuilder.scan("EMP")
        .project(relBuilder.field("DEPTNO"), relBuilder.field("ENAME"))
        .sort(relBuilder.field("DEPTNO")).build();

    MatcherAssert.assertThat(RelOptUtil.toString(relNode), CoreMatchers.equalTo(
        "LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "  LogicalProject(DEPTNO=[$7], ENAME=[$1])\n"
            + "    JdbcTableScan(table=[[  JDBC_SCOTT, EMP]])\n"));

    VolcanoPlanner planner = (VolcanoPlanner) relNode.getCluster().getPlanner();
    //获取期望的RelTraiset，这里是将Convention.None替换成EnumerableConvention
    RelTraitSet desired = relNode.getTraitSet().replace(EnumerableConvention.INSTANCE).simplify();
    //设置根节点，会从根节点开始迭代将所有子节点也注册到planner中
    planner.setRoot(planner.changeTraits(relNode, desired));
    RelNode result = planner.chooseDelegate().findBestExp();
    MatcherAssert.assertThat(RelOptUtil.toString(result), CoreMatchers.equalTo(
        "JdbcToEnumerableConverter\n"
        + "  JdbcSort(sort0=[$0], dir0=[ASC])\n"
        + "    JdbcProject(DEPTNO=[$7], ENAME=[$1])\n"
        + "      JdbcTableScan(table=[[  JDBC_SCOTT, EMP]])\n"));
  }

  @Test
  public void testVolcano() {
    SchemaPlus rootSchema = CalciteUtils.registerRootSchema();

    final FrameworkConfig fromworkConfig = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(rootSchema)
        .traitDefs(ConventionTraitDef.INSTANCE, RelDistributionTraitDef.INSTANCE)
        .build();

    String sql
        = "select u.id as user_id, u.name as user_name, j.company as user_company, u.age as user_age from users u"
        + " join jobs j on u.id=j.id where u.age > 30 and j.id>10 order by user_id";

    // use HepPlanner
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelDistributionTraitDef.INSTANCE);
    // add rules
    planner.addRule(FilterJoinRule.FilterIntoJoinRule.Config.DEFAULT.toRule());
    planner.addRule(ReduceExpressionsRule.ProjectReduceExpressionsRule.Config.DEFAULT.toRule());
    planner.addRule(PruneEmptyRules.PROJECT_INSTANCE);
    // add ConverterRule
    planner.addRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);

    try {
      SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      // sql parser
      SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
      SqlNode parsed = parser.parseStmt();

      CalciteCatalogReader calciteCatalogReader = new CalciteCatalogReader(
          CalciteSchema.from(rootSchema),
          CalciteSchema.from(rootSchema).path(null),
          factory,
          new CalciteConnectionConfigImpl(new Properties()));

      // sql validate
      SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), calciteCatalogReader,
          factory, fromworkConfig.getSqlValidatorConfig());
      SqlNode validated = validator.validate(parsed);

      final RexBuilder rexBuilder = CalciteUtils.createRexBuilder(factory);
      final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

      // init SqlToRelConverter config
      final SqlToRelConverter.Config config = SqlToRelConverter.configBuilder()
          .withConfig(fromworkConfig.getSqlToRelConverterConfig())
          .withTrimUnusedFields(false)
          .build();
      // SqlNode toRelNode
      final SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(new CalciteUtils.ViewExpanderImpl(),
          validator, calciteCatalogReader, cluster, fromworkConfig.getConvertletTable(), config);
      RelRoot root = sqlToRelConverter.convertQuery(validated, false, true);

      root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
      final RelBuilder relBuilder = config.getRelBuilderFactory().create(cluster, null);
      root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
      RelNode relNode = root.rel;

      RelTraitSet desiredTraits =
          relNode.getCluster().traitSet().replace(EnumerableConvention.INSTANCE);
      relNode = planner.changeTraits(relNode, desiredTraits);

      planner.setRoot(relNode);
      relNode = planner.findBestExp();
      System.out.println("-----------------------------------------------------------");
      System.out.println("The Best relational expression string:");
      System.out.println(RelOptUtil.toString(relNode));
      System.out.println("-----------------------------------------------------------");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void hepTest() {
    SchemaPlus rootSchema = CalciteUtils.registerRootSchema();

    final FrameworkConfig fromworkConfig = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(rootSchema)
        .traitDefs(ConventionTraitDef.INSTANCE, RelDistributionTraitDef.INSTANCE)
        .build();

    String sql
        = "select u.id as user_id, u.name as user_name, j.company as user_company, u.age as user_age from users u"
        + " join jobs j on u.id=j.id where u.age > 30 and j.id>10 order by user_id";

    // use HepPlanner
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleInstance(FilterJoinRule.FilterIntoJoinRule.Config.DEFAULT.toRule());
    builder.addRuleInstance(ReduceExpressionsRule.ProjectReduceExpressionsRule.Config.DEFAULT.toRule());
    builder.addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE);
    HepPlanner planner = new HepPlanner(builder.build());

    try {
      SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      // sql parser
      SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
      SqlNode parsed = parser.parseStmt();

      CalciteCatalogReader calciteCatalogReader = new CalciteCatalogReader(
          CalciteSchema.from(rootSchema),
          CalciteSchema.from(rootSchema).path(null),
          factory,
          new CalciteConnectionConfigImpl(new Properties()));

      // sql validate
      SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), calciteCatalogReader,
          factory, fromworkConfig.getSqlValidatorConfig());
      SqlNode validated = validator.validate(parsed);

      final RexBuilder rexBuilder = CalciteUtils.createRexBuilder(factory);
      final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

      // init SqlToRelConverter config
      final SqlToRelConverter.Config config = SqlToRelConverter.configBuilder()
          .withConfig(fromworkConfig.getSqlToRelConverterConfig())
          .withTrimUnusedFields(false)
          .build();
      // SqlNode toRelNode
      final SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(new CalciteUtils.ViewExpanderImpl(),
          validator, calciteCatalogReader, cluster, fromworkConfig.getConvertletTable(), config);
      RelRoot root = sqlToRelConverter.convertQuery(validated, false, true);

      root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
      final RelBuilder relBuilder = config.getRelBuilderFactory().create(cluster, null);
      root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
      RelNode relNode = root.rel;

      planner.setRoot(relNode);
      relNode = planner.findBestExp();
      System.out.println("-----------------------------------------------------------");
      System.out.println("The Best relational expression string:");
      System.out.println(RelOptUtil.toString(relNode));
      System.out.println("-----------------------------------------------------------");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
