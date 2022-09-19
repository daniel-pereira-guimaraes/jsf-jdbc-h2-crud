package jsf_jdbc_h2_crud;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class PersonDAO {

	private static final String SQL_CREATE_TABLE_PERSON = 
		"CREATE TABLE IF NOT EXISTS person("
		+ "  id INTEGER auto_increment, " + "  "
		+ "  first_name VARCHAR(20) NOT NULL, "
		+ "  last_name VARCHAR(30) NOT NULL)";
	private static final String SQL_INSERT = "INSERT INTO person(first_name, last_name) VALUES(?,?)";
	private static final String SQL_UPDATE = "UPDATE person SET first_name=?, last_name=? WHERE id=?";
	private static final String SQL_DELETE = "DELETE person WHERE id=?";
	private static final String SQL_SELECT = "SELECT id, first_name, last_name FROM person %s ORDER BY first_name, last_name";
	private static final String SQL_SELECT_BY_ID = "SELECT first_name, last_name FROM person WHERE id=?";

	private static PersonDAO instance;
	
	public static PersonDAO getInstance() throws ClassNotFoundException, SQLException, Exception {
		if (instance == null)
			instance = new PersonDAO();
		return instance;
	}
	
	private PersonDAO() throws ClassNotFoundException, SQLException, Exception {
		if (!isPrepared()) {
			prepareDatabase();
		}
	}
	
	private Connection getConnection() throws ClassNotFoundException, SQLException {
		Class.forName("org.h2.Driver");
		return DriverManager.getConnection("jdbc:h2:~/test", "sa", "");
	}

	private boolean isPrepared() throws Exception {
		Connection connection = null;
		ResultSet rs = null;
		try {
			connection = getConnection();
			rs = connection.getMetaData().getTables(null, null, "PERSON", null);
			return rs.next();
		} finally {
			Utils.close(rs, connection);
		}
	}

	private void prepareDatabase() throws Exception {
		Connection connection = null;
		Statement stm = null;
		try {
			connection = getConnection();
			stm = connection.createStatement();
			stm.execute(SQL_CREATE_TABLE_PERSON);
		} finally {
			Utils.close(stm, connection);
		}
	}
	
	public void insert(Person person) throws Exception {
		// Demonstration of explicit transaction control.
		Connection connection = null;
		PreparedStatement pstm = null;
		try {
			connection = getConnection();
			connection.setAutoCommit(false);
			pstm = connection.prepareStatement(SQL_INSERT);
			pstm.setString(1, person.getFirstName());
			pstm.setString(2, person.getLastName());
			pstm.execute();
			connection.commit();
		} catch(Exception e) {
			if (connection != null)
				connection.rollback();
			throw e;
		} finally {
			Utils.close(pstm, connection);
		}
	}

	public void update(Person person) throws Exception {
		// Explicit transaction not required for only one operation.
		Connection connection = null;
		PreparedStatement pstm = null;
		try {
			connection = getConnection();
			pstm = connection.prepareStatement(SQL_UPDATE);
			pstm.setString(1, person.getFirstName());
			pstm.setString(2, person.getLastName());
			pstm.setLong(3, person.getId());
			pstm.execute();
		} finally {
			Utils.close(pstm, connection);
		}
	}

	public void delete(Long id) throws Exception {
		Connection connection = null;
		PreparedStatement pstm = null;
		try {
			connection = getConnection();
			pstm = connection.prepareStatement(SQL_DELETE);
			pstm.setLong(1, id);
			pstm.execute();
		} finally {
			Utils.close(pstm, connection);
		}
	}

	public List<Person> selectByText(String text) throws Exception {
		String sql = SQL_SELECT;
		boolean hasFilter = text != null && !text.isEmpty();
		if (hasFilter) {
			text = "%" + text + "%";
			sql = String.format(sql, "WHERE first_name LIKE ? OR last_name LIKE ?");
		} else {
			sql = String.format(sql, "");			
		}
		System.out.println(sql);
		List<Person> list = new ArrayList<Person>();
		Connection connection = null;
		PreparedStatement pstm = null;
		ResultSet rs = null;
		try {
			connection = getConnection();
			pstm = connection.prepareStatement(sql);
			if (hasFilter) {
				pstm.setString(1, text);
				pstm.setString(2, text);
			}
			rs = pstm.executeQuery();
			while (rs.next())
				list.add(new Person(rs.getLong(1), rs.getString(2), rs.getString(3)));
		} finally {
			Utils.close(rs, pstm, connection);
		}
		return list;
	}
	
	public Person selectById(Long id) throws Exception {
		Connection connection = null;
		PreparedStatement pstm = null;
		ResultSet rs = null;
		try {
			connection = getConnection();
			pstm = connection.prepareStatement(SQL_SELECT_BY_ID);
			pstm.setLong(1, id);
			rs = pstm.executeQuery();
			if (rs.next())
				return new Person(id, rs.getString(1), rs.getString(2));
			else
				throw new Exception("Person not found: " + id);
		} finally {
			Utils.close(rs, pstm, connection);
		}
	}


}
