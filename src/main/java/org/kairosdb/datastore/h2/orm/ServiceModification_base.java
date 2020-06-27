package org.kairosdb.datastore.h2.orm;

import java.util.*;
import org.agileclick.genorm.runtime.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
	This class has been automatically generated by GenORMous.  This file
	should not be modified.
	
*/
@SuppressWarnings("serial")
public class ServiceModification_base extends GenOrmRecord
	{
	protected static final Logger s_logger = LoggerFactory.getLogger(ServiceModification.class.getName());

	public static final String COL_SERVICE = "service";
	public static final String COL_SERVICE_KEY = "service_key";
	public static final String COL_MODIFICATION_TIME = "modification_time";

	//Change this value to true to turn on warning messages
	private static final boolean WARNINGS = false;
	private static final String SELECT = "SELECT this.\"service\", this.\"service_key\", this.\"modification_time\" ";
	private static final String FROM = "FROM service_modification this ";
	private static final String WHERE = "WHERE ";
	private static final String KEY_WHERE = "WHERE \"service\" = ? AND \"service_key\" = ?";
	
	public static final String TABLE_NAME = "service_modification";
	public static final int NUMBER_OF_COLUMNS = 3;
	
	
	private static final String s_fieldEscapeString = "\""; 
	
	public static final GenOrmFieldMeta SERVICE_FIELD_META = new GenOrmFieldMeta("service", "string", 0, true, false);
	public static final GenOrmFieldMeta SERVICE_KEY_FIELD_META = new GenOrmFieldMeta("service_key", "string", 1, true, false);
	public static final GenOrmFieldMeta MODIFICATION_TIME_FIELD_META = new GenOrmFieldMeta("modification_time", "timestamp", 2, false, false);

	
		
	//===========================================================================
	public static ServiceModificationFactoryImpl factory = new ServiceModificationFactoryImpl();
	
	public static interface ServiceModificationFactory extends GenOrmRecordFactory
		{
		public boolean delete(String service, String serviceKey);
		public ServiceModification find(String service, String serviceKey);
		public ServiceModification findOrCreate(String service, String serviceKey);
		}
	
	public static class ServiceModificationFactoryImpl //Inherit interfaces
			implements ServiceModificationFactory 
		{
		public static final String CREATE_SQL = "CREATE CACHED TABLE service_modification (\n	\"service\" VARCHAR  NOT NULL,\n	\"service_key\" VARCHAR  NOT NULL,\n	\"modification_time\" TIMESTAMP  NULL,\n	PRIMARY KEY (\"service\", \"service_key\")\n	)";

		private ArrayList<GenOrmFieldMeta> m_fieldMeta;
		private ArrayList<GenOrmConstraint> m_foreignKeyConstraints;
		
		protected ServiceModificationFactoryImpl()
			{
			m_fieldMeta = new ArrayList<GenOrmFieldMeta>();
			m_fieldMeta.add(SERVICE_FIELD_META);
			m_fieldMeta.add(SERVICE_KEY_FIELD_META);
			m_fieldMeta.add(MODIFICATION_TIME_FIELD_META);

			m_foreignKeyConstraints = new ArrayList<GenOrmConstraint>();
			}
			
		protected ServiceModification newServiceModification(java.sql.ResultSet rs)
			{
			ServiceModification rec = new ServiceModification();
			((ServiceModification_base)rec).initialize(rs);
			return ((ServiceModification)GenOrmDataSource.getGenOrmConnection().getUniqueRecord(rec));
			}
	
		//---------------------------------------------------------------------------
		/**
			Returns a list of the feild meta for the class that this is a factory of
		*/
		public List<GenOrmFieldMeta> getFields()
			{
			return (m_fieldMeta);
			}

		//---------------------------------------------------------------------------
		/**
			Returns a list of foreign key constraints
		*/
		public List<GenOrmConstraint> getForeignKeyConstraints()
			{
			return (m_foreignKeyConstraints);
			}
			
		//---------------------------------------------------------------------------
		/**
			Returns the SQL create statement for this table
		*/
		public String getCreateStatement()
			{
			return (CREATE_SQL);
			}
			
		//---------------------------------------------------------------------------
		/**
			Creates a new entry with the specified primary keys.
		*/
		public ServiceModification create(String service, String serviceKey)
			{
			ServiceModification rec = new ServiceModification();
			rec.m_isNewRecord = true;
			
			((ServiceModification_base)rec).setService(service);
			((ServiceModification_base)rec).setServiceKey(serviceKey);

			
			return ((ServiceModification)GenOrmDataSource.getGenOrmConnection().getUniqueRecord(rec));
			}
		//---------------------------------------------------------------------------
		/**
			Creates a new entry that is empty
		*/
		public ServiceModification createRecord()
			{
			ServiceModification rec = new ServiceModification();
			rec.m_isNewRecord = true;
			
			return ((ServiceModification)GenOrmDataSource.getGenOrmConnection().getUniqueRecord(rec));
			}
			
		//---------------------------------------------------------------------------
		/**
		If the table has a primary key that has a key generator this method will 
		return a new table entry with a generated primary key.
		@return ServiceModification with generated primary key
		*/
		public ServiceModification createWithGeneratedKey()
			{
			throw new UnsupportedOperationException("ServiceModification does not support a generated primary key");
			}
			
		//---------------------------------------------------------------------------
		/**
		A generic api for finding a record.
		@param keys This must match the primary key for this record.  If the 
		record has multiple primary keys this parameter must be of type Object[] 
		where each element is the corresponding key.
		@return ServiceModification or null if no record is found
		*/
		public ServiceModification findRecord(Object keys)
			{
			Object[] kArr = (Object[])keys;
			return (find((String)kArr[0], (String)kArr[1]));
			}
			
		//---------------------------------------------------------------------------
		/**
			Deletes the record with the specified primary keys.
			The point of this api is to prevent a hit on the db to see if the record
			is there.  This call will add a record to the next transaction that is 
			marked for delete. 
			
			@return Returns true if the record was previous created and existed
			either in the transaction cache or the db.
		*/
		public boolean delete(String service, String serviceKey)
			{
			boolean ret = false;
			ServiceModification rec = new ServiceModification();
			
			((ServiceModification_base)rec).initialize(service, serviceKey);
			GenOrmConnection con = GenOrmDataSource.getGenOrmConnection();
			ServiceModification cachedRec = (ServiceModification)con.getCachedRecord(rec.getRecordKey());
			
			if (cachedRec != null)
				{
				ret = true;
				cachedRec.delete();
				}
			else
				{
				rec = (ServiceModification)con.getUniqueRecord(rec);  //This adds the record to the cache
				rec.delete();
				ret = rec.flush();
				rec.setIgnored(true); //So the system does not try to delete it again at commmit
				}
				
			return (ret);
			}
			
		//---------------------------------------------------------------------------
		/**
		Find the record with the specified primary keys
		@return ServiceModification or null if no record is found
		*/
		public ServiceModification find(String service, String serviceKey)
			{
			ServiceModification rec = new ServiceModification();
			
			//Create temp object and look in cache for it
			((ServiceModification_base)rec).initialize(service, serviceKey);
			rec = (ServiceModification)GenOrmDataSource.getGenOrmConnection().getCachedRecord(rec.getRecordKey());
			
			java.sql.PreparedStatement genorm_statement = null;
			java.sql.ResultSet genorm_rs = null;
			
			if (rec == null)
				{
				try
					{
					//No cached object so look in db
					genorm_statement = GenOrmDataSource.prepareStatement(SELECT+FROM+KEY_WHERE);
					genorm_statement.setString(1, service);
					genorm_statement.setString(2, serviceKey);

					s_logger.debug(genorm_statement.toString());
						
					genorm_rs = genorm_statement.executeQuery();
					if (genorm_rs.next())
						rec = newServiceModification(genorm_rs);
					}
				catch (java.sql.SQLException sqle)
					{
					throw new GenOrmException(sqle);
					}
				finally
					{
					try
						{
						if (genorm_rs != null)
							genorm_rs.close();
							
						if (genorm_statement != null)
							genorm_statement.close();
						}
					catch (java.sql.SQLException sqle2)
						{
						throw new GenOrmException(sqle2);
						}
					}
				}
				
			return (rec);
			}
		
		//---------------------------------------------------------------------------
		/**
		This is the same as find except if the record returned is null a new one 
		is created with the specified primary keys
		@return A new or existing record.  
		*/
		public ServiceModification findOrCreate(String service, String serviceKey)
			{
			ServiceModification rec = find(service, serviceKey);
			if (rec == null)
				rec = create(service, serviceKey);
				
			return (rec);
			}
			
		//---------------------------------------------------------------------------
		/**
			Convenience method for selecting records.  Ideally this should not be use, 
			instead a custom query for this table should be used.
			@param where sql where statement.
		*/
		public ResultSet select(String where)
			{
			return (select(where, null));
			}
			
		//---------------------------------------------------------------------------
		/**
			Convenience method for selecting records.  Ideally this should not be use, 
			instead a custom query for this table should be used.
			@param where sql where statement.
			@param orderBy sql order by statement
		*/
		public ResultSet select(String where, String orderBy)
			{
			ResultSet rs = null;
			java.sql.Statement stmnt = null;
			
			try
				{
				stmnt = GenOrmDataSource.createStatement();
				StringBuilder sb = new StringBuilder();
				sb.append(SELECT);
				sb.append(FROM);
				if (where != null)
					{
					sb.append(WHERE);
					sb.append(where);
					}
					
				if (orderBy != null)
					{
					sb.append(" ");
					sb.append(orderBy);
					}
				
				String query = sb.toString();
				rs = new SQLResultSet(stmnt.executeQuery(query), query, stmnt);
				}
			catch (java.sql.SQLException sqle)
				{
				try
					{
					if (stmnt != null)
						stmnt.close();
					}
				catch (java.sql.SQLException sqle2) { }
					
				throw new GenOrmException(sqle);
				}
				
			return (rs);
			}
			
		
		//---------------------------------------------------------------------------
		/**
			Calls all query methods with test parameters.
		*/
		public void testQueryMethods()
			{
			ResultSet rs;
			}
		}
		
	//===========================================================================
	public static interface ResultSet extends GenOrmResultSet
		{
		public ArrayList<ServiceModification> getArrayList(int maxRows);
		public ArrayList<ServiceModification> getArrayList();
		public ServiceModification getRecord();
		public ServiceModification getOnlyRecord();
		}
		
	//===========================================================================
	private static class SQLResultSet 
			implements ResultSet
		{
		private java.sql.ResultSet m_resultSet;
		private java.sql.Statement m_statement;
		private String m_query;
		private boolean m_onFirstResult;
		
		//------------------------------------------------------------------------
		protected SQLResultSet(java.sql.ResultSet resultSet, String query, java.sql.Statement statement)
			{
			m_resultSet = resultSet;
			m_statement = statement;
			m_query = query;
			m_onFirstResult = false;
			}
		
		//------------------------------------------------------------------------
		/**
			Closes any underlying java.sql.Result set and java.sql.Statement 
			that was used to create this results set.
		*/
		public void close()
			{
			try
				{
				m_resultSet.close();
				m_statement.close();
				}
			catch (java.sql.SQLException sqle)
				{
				throw new GenOrmException(sqle);
				}
			}
			
		//------------------------------------------------------------------------
		/**
			Returns the reults as an ArrayList of Record objects.
			The Result set is closed within this call
			@param maxRows if the result set contains more than this param
				then an exception is thrown
		*/
		public ArrayList<ServiceModification> getArrayList(int maxRows)
			{
			ArrayList<ServiceModification> results = new ArrayList<ServiceModification>();
			int count = 0;
			
			try
				{
				if (m_onFirstResult)
					{
					count ++;
					results.add(factory.newServiceModification(m_resultSet));
					}
					
				while (m_resultSet.next() && (count < maxRows))
					{
					count ++;
					results.add(factory.newServiceModification(m_resultSet));
					}
					
				if (m_resultSet.next())
					throw new GenOrmException("Bound of "+maxRows+" is too small for query ["+m_query+"]");
				}
			catch (java.sql.SQLException sqle)
				{
				sqle.printStackTrace();
				throw new GenOrmException(sqle);
				}
				
			close();
			return (results);
			}
		
		//------------------------------------------------------------------------
		/**
			Returns the reults as an ArrayList of Record objects.
			The Result set is closed within this call
		*/
		public ArrayList<ServiceModification> getArrayList()
			{
			ArrayList<ServiceModification> results = new ArrayList<ServiceModification>();
			
			try
				{
				if (m_onFirstResult)
					results.add(factory.newServiceModification(m_resultSet));
					
				while (m_resultSet.next())
					results.add(factory.newServiceModification(m_resultSet));
				}
			catch (java.sql.SQLException sqle)
				{
				sqle.printStackTrace();
				throw new GenOrmException(sqle);
				}
				
			close();
			return (results);
			}
			
		//------------------------------------------------------------------------
		/**
			Returns the underlying java.sql.ResultSet object
		*/
		public java.sql.ResultSet getResultSet()
			{
			return (m_resultSet);
			}
			
		//------------------------------------------------------------------------
		/**
			Returns the current record in the result set
		*/
		public ServiceModification getRecord()
			{
			return (factory.newServiceModification(m_resultSet));
			}
			
		//------------------------------------------------------------------------
		/**
			This call expects only one record in the result set.  If multiple records
			are found an excpetion is thrown.
			The ResultSet object is automatically closed by this call.
		*/
		public ServiceModification getOnlyRecord()
			{
			ServiceModification ret = null;
			
			try
				{
				if (m_resultSet.next())
					ret = factory.newServiceModification(m_resultSet);
					
				if (m_resultSet.next())
					throw new GenOrmException("Multiple rows returned in call from ServiceModification.getOnlyRecord");
				}
			catch (java.sql.SQLException sqle)
				{
				throw new GenOrmException(sqle);
				}
				
			close();
			return (ret);
			}
			
		//------------------------------------------------------------------------
		/**
			Returns true if there is another record in the result set.
		*/
		public boolean next()
			{
			boolean ret = false;
			m_onFirstResult = true;
			try
				{
				ret = m_resultSet.next();
				}
			catch (java.sql.SQLException sqle)
				{
				throw new GenOrmException(sqle);
				}
			
			return (ret);
			}
		}
		
	//===========================================================================
		
	private GenOrmString m_service;
	private GenOrmString m_serviceKey;
	private GenOrmTimestamp m_modificationTime;

	
	private List<GenOrmRecordKey> m_foreignKeys;
	
	public List<GenOrmRecordKey> getForeignKeys() { return (m_foreignKeys); }


	//---------------------------------------------------------------------------
	/**
	*/
	public String getService() { return (m_service.getValue()); }
	public ServiceModification setService(String data)
		{
		boolean changed = m_service.setValue(data);
		
		//Add the now dirty record to the transaction only if it is not previously dirty
		if (changed)
			{
			if (m_dirtyFlags.isEmpty())
				GenOrmDataSource.getGenOrmConnection().addToTransaction(this);
				
			m_dirtyFlags.set(SERVICE_FIELD_META.getDirtyFlag());
			
			if (m_isNewRecord) //Force set the prev value
				m_service.setPrevValue(data);
			}
			
		return ((ServiceModification)this);
		}
		

	//---------------------------------------------------------------------------
	/**
	*/
	public String getServiceKey() { return (m_serviceKey.getValue()); }
	public ServiceModification setServiceKey(String data)
		{
		boolean changed = m_serviceKey.setValue(data);
		
		//Add the now dirty record to the transaction only if it is not previously dirty
		if (changed)
			{
			if (m_dirtyFlags.isEmpty())
				GenOrmDataSource.getGenOrmConnection().addToTransaction(this);
				
			m_dirtyFlags.set(SERVICE_KEY_FIELD_META.getDirtyFlag());
			
			if (m_isNewRecord) //Force set the prev value
				m_serviceKey.setPrevValue(data);
			}
			
		return ((ServiceModification)this);
		}
		

	//---------------------------------------------------------------------------
	/**
	*/
	public java.sql.Timestamp getModificationTime() { return (m_modificationTime.getValue()); }
	public ServiceModification setModificationTime(java.sql.Timestamp data)
		{
		boolean changed = m_modificationTime.setValue(data);
		
		//Add the now dirty record to the transaction only if it is not previously dirty
		if (changed)
			{
			if (m_dirtyFlags.isEmpty())
				GenOrmDataSource.getGenOrmConnection().addToTransaction(this);
				
			m_dirtyFlags.set(MODIFICATION_TIME_FIELD_META.getDirtyFlag());
			
			if (m_isNewRecord) //Force set the prev value
				m_modificationTime.setPrevValue(data);
			}
			
		return ((ServiceModification)this);
		}
		
	public boolean isModificationTimeNull()
		{
		return (m_modificationTime.isNull());
		}
		
	public ServiceModification setModificationTimeNull()
		{
		boolean changed = m_modificationTime.setNull();
		
		if (changed)
			{
			if (m_dirtyFlags.isEmpty())
				GenOrmDataSource.getGenOrmConnection().addToTransaction(this);
				
			m_dirtyFlags.set(MODIFICATION_TIME_FIELD_META.getDirtyFlag());
			}
		
		return ((ServiceModification)this);
		}
	
	
	
	
	//---------------------------------------------------------------------------
	protected void initialize(String service, String serviceKey)
		{
		m_service.setValue(service);
		m_service.setPrevValue(service);
		m_serviceKey.setValue(serviceKey);
		m_serviceKey.setPrevValue(serviceKey);

		}
		
	//---------------------------------------------------------------------------
	protected void initialize(java.sql.ResultSet rs)
		{
		try
			{
			if (s_logger.isDebugEnabled())
				{
				java.sql.ResultSetMetaData meta = rs.getMetaData();
				for (int I = 1; I <= meta.getColumnCount(); I++)
					{
					s_logger.debug("Reading - "+meta.getColumnName(I) +" : "+rs.getString(I));
					}
				}
			m_service.setValue(rs, 1);
			m_serviceKey.setValue(rs, 2);
			m_modificationTime.setValue(rs, 3);

			}
		catch (java.sql.SQLException sqle)
			{
			throw new GenOrmException(sqle);
			}
		}
	
	//---------------------------------------------------------------------------
	/*package*/ ServiceModification_base()
		{
		super(TABLE_NAME);
		m_logger = s_logger;
		m_foreignKeys = new ArrayList<GenOrmRecordKey>();
		m_dirtyFlags = new java.util.BitSet(NUMBER_OF_COLUMNS);
		

		m_service = new GenOrmString(SERVICE_FIELD_META);
		addField(COL_SERVICE, m_service);

		m_serviceKey = new GenOrmString(SERVICE_KEY_FIELD_META);
		addField(COL_SERVICE_KEY, m_serviceKey);

		m_modificationTime = new GenOrmTimestamp(MODIFICATION_TIME_FIELD_META);
		addField(COL_MODIFICATION_TIME, m_modificationTime);

		GenOrmRecordKey foreignKey;
		}
	
	//---------------------------------------------------------------------------
	@Override
	public GenOrmConnection getGenOrmConnection()
		{
		return (GenOrmDataSource.getGenOrmConnection());
		}
		
	//---------------------------------------------------------------------------
	@Override
	public String getFieldEscapeString()
		{
		return (s_fieldEscapeString);
		}
		
	//---------------------------------------------------------------------------
	@Override
	public void setMTS()
		{
		}
		
	//---------------------------------------------------------------------------
	@Override
	public void setCTS()
		{
		}
		
	//---------------------------------------------------------------------------
	public String toString()
		{
		StringBuilder sb = new StringBuilder();
		
		sb.append("service=\"");
		sb.append(m_service.getValue());
		sb.append("\" ");
		sb.append("service_key=\"");
		sb.append(m_serviceKey.getValue());
		sb.append("\" ");
		sb.append("modification_time=\"");
		sb.append(m_modificationTime.getValue());
		sb.append("\" ");

		
		return (sb.toString().trim());
		}
		
	//===========================================================================

	
	
	}
	
	