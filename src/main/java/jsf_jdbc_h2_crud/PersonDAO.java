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
	private static final String SQL_SELECT_BY_ID = "SELECT first_name, last_name FROM person WHERE id=?";
	private static final String SQL_SELECT_ALL = "SELECT id, first_name, last_name FROM person ORDER BY first_name, last_name";

	private static PersonDAO instance;
	
	public static PersonDAO getInstante() throws ClassNotFoundException, SQLException, Exception {
		if (instance == null)
			instance = new PersonDAO();
		return instance;
	}
	
	private PersonDAO() throws ClassNotFoundException, SQLException, Exception {
		if (!isPrepared()) {
			prepareDatabase();
			insert(new Person("Daniel", "Guimarães"));
			insert(new Person("João", "Silva"));
			insert(new Person("José", "Souza"));
		}
	}
	
	private Connection getConnection() throws ClassNotFoundException, SQLException {
		Class.forName("org.h2.Driver");
		return DriverManager.getConnection("jdbc:h2:~/test", "sa", "");
	}

	private boolean isPrepared() throws SQLException, ClassNotFoundException {
		Connection connection = null;
		ResultSet rs = null;
		try {
			connection = getConnection();
			rs = connection.getMetaData().getTables(null, null, "PERSON", null);
			return rs.next();
		} finally {
			if (rs != null)
				rs.close();
			if (connection != null)
				connection.close();
		}
	}

	private void prepareDatabase() throws SQLException, ClassNotFoundException {
		Connection connection = getConnection();
		try {
			Statement stm = connection.createStatement();
			try {
				stm.execute(SQL_CREATE_TABLE_PERSON);
			} finally {
				stm.close();
			}
		} finally {
			connection.close();
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
			if (pstm != null)
				pstm.close();
			if (connection != null)
				connection.close();
		}
	}

	public void update(Person person) throws Exception {
		// Explicit transaction not required for only one operation.
		Connection connection = getConnection();
		try {
			PreparedStatement pstm = connection.prepareStatement(SQL_UPDATE);
			try {
				pstm.setString(1, person.getFirstName());
				pstm.setString(2, person.getLastName());
				pstm.setLong(3, person.getId());
				pstm.execute();
			} finally {
				pstm.close();
			}
		} finally {
			connection.close();
		}
	}

	public void delete(Long id) throws ClassNotFoundException, SQLException {
		Connection connection = getConnection();
		try {
			PreparedStatement pstm = connection.prepareStatement(SQL_DELETE);
			try {
				pstm.setLong(1, id);
				pstm.execute();
			} finally {
				pstm.close();
			}
		} finally {
			connection.close();
		}
	}

	public Person selectById(Long id) throws Exception {
		Person person = null;
		Connection connection = null;
		PreparedStatement pstm = null;
		ResultSet rs = null;
		try {
			connection = getConnection();
			pstm = connection.prepareStatement(SQL_SELECT_BY_ID);
			pstm.setLong(1, id);
			rs = pstm.executeQuery();
			if (rs.next())
				person = new Person(id, rs.getString(1), rs.getString(2));
			else
				throw new Exception("Person not found: " + id);
		} finally {
			if (rs != null)	rs.close();
			if (pstm != null) pstm.close();
			if (connection != null)	connection.close();
		}
		return person;
	}

	public List<Person> selectAll() throws ClassNotFoundException, SQLException {
		List<Person> list = new ArrayList<Person>();
		Connection connection = null;
		Statement stm = null;
		ResultSet rs = null;
		try {
			connection = getConnection();
			stm = connection.createStatement();
			rs = stm.executeQuery(SQL_SELECT_ALL);
			while (rs.next())
				list.add(new Person(rs.getLong(1), rs.getString(2), rs.getString(3)));
		} finally {
			if (rs != null) rs.close();
			if (stm != null) stm.close();
			if (connection != null) connection.close();
		}
		return list;
	}

}
