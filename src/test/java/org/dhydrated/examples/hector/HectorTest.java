package org.dhydrated.examples.hector;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class HectorTest {
	
	private static Log LOG = LogFactory.getLog(HectorTest.class);

	private static final String CLUSTER_NAME = "Test Cluster";
	private static final String HOST = "localhost";
	private static final String PORT = "9160";
	private static final String KEYSPACE = "Keyspace2";
	private static final String COLUMN_FAMILY = "Users";
	private static final String COLUMN_EMAIL = "email";
	private static final String COLUMN_FIRSTNAME = "firstname";
	private static final String COLUMN_DOB = "dob";
	private static final String COLUMN_STATUS = "status";
	private static final String REPLICATION_STRATEGY = "org.apache.cassandra.locator.SimpleStrategy";
	
	private static final String KEY_1 = "dhydrated";
	private static final String EMAIL_1 = "dhydrated@gmail.com";
	private static final String DOB_1 = "1999-02-01";
	private static final String STATUS_1 = "disabled";

	private static final String KEY_2 = "cassandra";
	private static final String EMAIL_2 = "cassandra@gmail.com";
	private static final String FIRSTNAME_2 = "cassandra";
	
	private Cluster cluster = null;
	
	@BeforeClass
	public static void createKeyspace() throws Exception{
		
		Cluster cluster = HFactory.getOrCreateCluster(CLUSTER_NAME, HOST + ":" + PORT);
		
		KeyspaceDefinition keyspaceDef = HFactory.createKeyspaceDefinition(KEYSPACE, REPLICATION_STRATEGY, 1, null);
		
		cluster.addKeyspace(keyspaceDef);
		
		Keyspace keyspace = HFactory.createKeyspace(KEYSPACE, cluster);
		
		Assert.assertEquals(KEYSPACE, keyspace.getKeyspaceName());
	}
	
	@Before
	public void setUp(){
		
		cluster = HFactory.getOrCreateCluster(CLUSTER_NAME, HOST + ":" + PORT);
	}
	
	
	
	@Test
	public void createColumnFamily() throws Exception {

		ColumnFamilyDefinition columnFamilyDef = HFactory.createColumnFamilyDefinition(KEYSPACE, COLUMN_FAMILY);
		
		cluster.addColumnFamily(columnFamilyDef);
	}
	
	@Test
	public void insertAndGetDataToColumnFamily() throws Exception {
		
		Keyspace keyspace = HFactory.createKeyspace(KEYSPACE, cluster);
		
		Serializer<String> serializer = StringSerializer.get();

		//insert
		Mutator<String> mutator = HFactory.createMutator(keyspace, serializer);
		mutator.insert(KEY_1, COLUMN_FAMILY,
				HFactory.createColumn(COLUMN_EMAIL, EMAIL_1, serializer, serializer));
		
		//get
		ColumnQuery<String, String, String> q = HFactory.createColumnQuery(keyspace,
				serializer, serializer, serializer);
		QueryResult<HColumn<String, String>> result = q.setKey(KEY_1).setName(COLUMN_EMAIL).setColumnFamily(COLUMN_FAMILY).execute();
		HColumn<String, String> column = result.get();
		
		Assert.assertEquals(COLUMN_EMAIL, column.getName());
		Assert.assertEquals(EMAIL_1, column.getValue());
	}
	
	@Test
	public void multiInsertAndGetDataToColumnFamily() throws Exception {
		
		Keyspace keyspace = HFactory.createKeyspace(KEYSPACE, cluster);
		
		Serializer<String> serializer = StringSerializer.get();

		//multi insert
		Mutator<String> mutator = HFactory.createMutator(keyspace, serializer);
		mutator.addInsertion(
					KEY_2,
					COLUMN_FAMILY,
					HFactory.createColumn(COLUMN_FIRSTNAME, FIRSTNAME_2,
							keyspace.createClock(), serializer, serializer));
		mutator.addInsertion(
				KEY_2,
				COLUMN_FAMILY,
				HFactory.createColumn(COLUMN_EMAIL, EMAIL_2,
						keyspace.createClock(), serializer, serializer));
		mutator.addInsertion(
				KEY_1,
				COLUMN_FAMILY,
				HFactory.createColumn(COLUMN_DOB, DOB_1,
						keyspace.createClock(), serializer, serializer));
							
		mutator.execute();
		
		//get
		ColumnQuery<String, String, String> q = HFactory.createColumnQuery(keyspace,
				serializer, serializer, serializer);
		QueryResult<HColumn<String, String>> result = q.setKey(KEY_2).setName(COLUMN_EMAIL).setColumnFamily(COLUMN_FAMILY).execute();
		HColumn<String, String> column = result.get();
		
		LOG.debug("test: " + column);
		
		Assert.assertEquals(COLUMN_EMAIL, column.getName());
		Assert.assertEquals(EMAIL_2, column.getValue());
	}

	
	@Test(expected=HInvalidRequestException.class)
	public void deleteDataToColumnFamily() throws Exception {
		
		Keyspace keyspace = HFactory.createKeyspace(KEYSPACE, cluster);
		
		Serializer<String> serializer = StringSerializer.get();

		//insert
		Mutator<String> insertMutator = HFactory.createMutator(keyspace, serializer);
		insertMutator.insert(KEY_1, COLUMN_FAMILY,
				HFactory.createColumn(COLUMN_STATUS, STATUS_1, serializer, serializer));
		
		//get
		ColumnQuery<String, String, String> query = HFactory.createColumnQuery(keyspace,
				serializer, serializer, serializer);
		QueryResult<HColumn<String, String>> result = query.setKey(KEY_1).setName(COLUMN_STATUS).setColumnFamily(COLUMN_FAMILY).execute();
		HColumn<String, String> column = result.get();
		
		Assert.assertEquals(COLUMN_STATUS, column.getName());
		Assert.assertEquals(STATUS_1, column.getValue());
		
		//delete
		Mutator<String> deleteMutator = HFactory.createMutator(keyspace, serializer);
		deleteMutator.addDeletion(KEY_1, COLUMN_FAMILY, COLUMN_STATUS, serializer);
		deleteMutator.execute();

		//get
		result = query.setKey(KEY_1).setName(COLUMN_STATUS).setColumnFamily(STATUS_1).execute();
	}
	
	@After
	public void tearDown(){
		
		cluster = null;
	}
	
	@AfterClass
	public static void dropKeyspace() throws Exception {
		
		Cluster cluster = HFactory.getOrCreateCluster(CLUSTER_NAME, HOST + ":" + PORT);
		
		cluster.dropKeyspace(KEYSPACE);
	}
}
