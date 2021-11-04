import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Principal {

    public static int calcular(String usuario){
        int cantidad = 0;
        String url = "jdbc:sqlserver://localhost;databaseName=Chat;integratedSecurity=true";
        try(Connection cnx = DriverManager.getConnection(url)){
            if(! cnx.isClosed()){
                Statement stmt = cnx.createStatement();
                String sentencia = "SELECT count(*) FROM Mensajes WHERE receptor=" + Integer.parseInt(usuario);
                ResultSet rs = stmt.executeQuery(sentencia);
                rs.next();
                cantidad = rs.getInt(1);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return cantidad;
    }

    public static List informacion(String fechaInicio, String fechaFin){
        List<Map> lista = new ArrayList<>();
        Map<String, Object> fila = null;
        String url = "jdbc:sqlserver://localhost;databaseName=Chat;integratedSecurity=true";
        try(Connection cnx = DriverManager.getConnection(url)){
            if(! cnx.isClosed()){
                String sentencia = "SELECT * FROM Mensajes WHERE fecha BETWEEN ? AND ?";
                PreparedStatement pstmt = cnx.prepareStatement(sentencia);
                ParameterMetaData pmd = pstmt.getParameterMetaData();

                pstmt.setTimestamp(1, Timestamp.valueOf(fechaInicio));
                pstmt.setTimestamp(2, Timestamp.valueOf(fechaFin));
                ResultSet rs = pstmt.executeQuery();
                ResultSetMetaData rsmd = rs.getMetaData();
                while(rs.next()){
                    fila = new HashMap<>();
                    for(int i = 1; i <= rsmd.getColumnCount(); i++){
                        fila.put(rsmd.getColumnName(i), rs.getObject(i));
                    }
                    lista.add(fila);
                }
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return lista;
    }

    public static boolean papelera(int codigo){
        boolean sw = false;
        String url = "jdbc:sqlserver://localhost;databaseName=Chat;integratedSecurity=true";
        try(Connection cnx = DriverManager.getConnection(url)){
            if(! cnx.isClosed()){
                Statement stmt = cnx.createStatement();
                String sentencia = "UPDATE Mensajes SET papelera = 'True' WHERE idMensaje=" + codigo;
                int filas = stmt.executeUpdate(sentencia);
                if(filas > 0)
                    sw = true;
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return sw;
    }

    public static boolean eliminar(int codigo){
        boolean sw = false;
        String url = "jdbc:sqlserver://localhost;databaseName=Chat;integratedSecurity=true";
        try(Connection cnx = DriverManager.getConnection(url)){
            if(! cnx.isClosed()){
                Statement stmt = cnx.createStatement();
                String sentencia = "DELETE FROM Mensajes WHERE idMensaje=" + codigo;
                int filas = stmt.executeUpdate(sentencia);
                if(filas > 0)
                    sw = true;
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return sw;
    }

    public static void main(String[] args){
        // Apartado A
        int mensajes = calcular("2");
        System.out.println(mensajes);

        // Apartado B
        List<Map> lista = informacion("2021-11-01 15:00:00", "2021-12-01 17:00:00");
        for(Map<String, Object> m : lista){
            for(Map.Entry<String, Object> fila : m.entrySet()){
                System.out.println(fila.getKey() + ": " + fila.getValue());
            }
        }

        // Apartado C
        boolean sw = papelera(2);
        if(sw)
            System.out.println("Mensaje enviado a la papelera correctamente");
        else
            System.out.println("No existe ningún mensaje con ese código");

        sw = false;

        // Apartado D
        sw = eliminar(1);
        if(sw)
            System.out.println("Mensaje eliminado correctamente");
        else
            System.out.println("No existe ningún mensaje con ese código");
    }

}
