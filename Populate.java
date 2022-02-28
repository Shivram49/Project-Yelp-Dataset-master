/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package JavaAccess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Date;
import java.text.DecimalFormat;
import java.util.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.server.ObjID;
import java.util.Iterator;
import org.json.JSONTokener;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.List;
import java.util.Arrays;

/**
 *
 * /**
 *
 * @author lizilina
 */
public class Populate {

    /**
     * @param args the command line arguments
     */
    public static final String DBURL = "jdbc:oracle:thin:@localhost:1521:xe";
    public static final String DBUSER = "system";
    public static final String DBPASS = "oracle";

    public static String yelp_user_file = null;
    public static String yelp_review_file = null;
    public static String yelp_business_file = null;
    public static String yelp_checkin_file = null;

    public static ArrayList<String> test_attrib_list = new ArrayList<String>();
    public static ArrayList<String> test_attrib_true = new ArrayList<String>();
    public static ArrayList<String> big_attrib_tester = new ArrayList<String>();

    public static void main(String[] args) throws SQLException {

        System.out.println("Working Directory = "
                + System.getProperty("user.dir"));

        int iteration_check = 0;
        for (String s : args) {
            iteration_check++;
            if (iteration_check == 1) {
                yelp_business_file = s;
            }
            if (iteration_check == 2) {
                yelp_review_file = s;
            }
            if (iteration_check == 3) {
                yelp_checkin_file = s;
            }
            if (iteration_check == 4) {
                yelp_user_file = s;
            }
        }

        System.out.println(":" + yelp_business_file + ":" + yelp_checkin_file + ":" + yelp_review_file);

        if (iteration_check == 0) {
            System.out.println("No command line arguments. Exiting...");
            return;
        }

        drop_all_tables();
        parse_users();
        parse_business();
        parse_attributes_and_categories();
        parse_reviews();
        parse_checkins();
    }

    public static void drop_all_tables() {
        try {
            PreparedStatement deleteTableDtataStmt = null;

            Class.forName("oracle.jdbc.driver.OracleDriver");
            Connection con = null;
            con = DriverManager.getConnection(DBURL, DBUSER, DBPASS);

            deleteTableDtataStmt = con.prepareStatement("TRUNCATE TABLE YELP_USER");
            deleteTableDtataStmt.executeUpdate();
            deleteTableDtataStmt.close();

            deleteTableDtataStmt = con.prepareStatement("ALTER TABLE BUSINESS DISABLE CONSTRAINT BUSINESS_PK CASCADE");
            deleteTableDtataStmt.executeUpdate();
            deleteTableDtataStmt.close();

            deleteTableDtataStmt = con.prepareStatement("TRUNCATE TABLE BUSINESS");
            deleteTableDtataStmt.executeUpdate();
            deleteTableDtataStmt.close();
            deleteTableDtataStmt = con.prepareStatement("ALTER TABLE BUSINESS ENABLE CONSTRAINT BUSINESS_PK");
            deleteTableDtataStmt.executeUpdate();
            deleteTableDtataStmt.close();

            deleteTableDtataStmt = con.prepareStatement("TRUNCATE TABLE BUSINESS_CATEGORY");
            deleteTableDtataStmt.executeUpdate();
            deleteTableDtataStmt.close();
            deleteTableDtataStmt = con.prepareStatement("TRUNCATE TABLE BUSINESS_SUB_CATEGORY");
            deleteTableDtataStmt.executeUpdate();
            deleteTableDtataStmt.close();
            deleteTableDtataStmt = con.prepareStatement("TRUNCATE TABLE REVIEWS");
            deleteTableDtataStmt.executeUpdate();
            deleteTableDtataStmt.close();

            deleteTableDtataStmt = con.prepareStatement("ALTER TABLE REVIEWS ENABLE CONSTRAINT FK_BUSINESSREV");
            deleteTableDtataStmt.executeUpdate();
            deleteTableDtataStmt.close();

            deleteTableDtataStmt = con.prepareStatement("TRUNCATE TABLE NATIVE_ATTRIBUTE");
            deleteTableDtataStmt.executeUpdate();
            deleteTableDtataStmt.close();
            deleteTableDtataStmt = con.prepareStatement("TRUNCATE TABLE YELP_USER");
            deleteTableDtataStmt.executeUpdate();
            deleteTableDtataStmt.close();

            con.close();;
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public static void parse_checkins() {
        try {
            System.out.println(yelp_checkin_file);
            File file_user = new File(yelp_checkin_file);
            FileReader fileReader_user = new FileReader(file_user);
            BufferedReader bufferedReader_user = new BufferedReader(fileReader_user);
            String line_user;
            String bid;
            PreparedStatement statement;
            int tester = 0;
            JSONObject jObj;
            JSONObject obj;

            Class.forName("oracle.jdbc.driver.OracleDriver");
            Connection con = null;

            con = DriverManager.getConnection(DBURL, DBUSER, DBPASS);

            while ((line_user = bufferedReader_user.readLine()) != null) {

                int[] days_time = {0,0,0,0,0,0,0};

                tester++;
                jObj = (JSONObject) new JSONTokener(line_user)
                        .nextValue();
                obj = jObj.getJSONObject("checkin_info");
                bid = jObj.getString("business_id");

                Iterator<?> keys = obj.keys();
                while (keys.hasNext()) {
                    String key = (String) keys.next();
                    int check_me = obj.getInt(key);
                    int day = Integer.parseInt(key.substring(key.length() -1, key.length()));
                    days_time[day] += check_me;
                }

                String me = "" + Integer.toString(days_time[0]);
                for (int i = 1; i < days_time.length; i++) {
                    me += ":" + Integer.toString(days_time[i]);
                }

                //System.out.println(me);

                statement = con.prepareStatement("UPDATE BUSINESS SET CHECKIN = '" +  me + "' WHERE BID = '" + bid + "'");
                statement.executeUpdate();
                statement.close();

            }
            con.close();
            fileReader_user.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, ex);
        } catch (JSONException ex) {
            Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public static void parse_attributes_and_categories() {
        String[] Main_categories = {"Transportation", "Shopping", "Active Life", "Arts & Entertainment", "Automotive", "Car Rental", "Cafes", "Beauty & Spas", "Convenience Stores", "Department Stores", "Education", "Event Planning & Services", "Flowers & Gifts", "Food", "Health & Medical", "Home Services", "Home & Garden", "Hospitals", "Hotels & Travel", "Hardware Stores", "Grocery", "Medical Centers", "Nurseries & Gardening", "Nightlife", "Restaurants", "Drugstores", "Dentists", "Doctors"};
        List valid_maincategories = Arrays.asList(Main_categories);
        String name = "";

        try {
            File file_user = new File(yelp_business_file);
            FileReader fileReader_user = new FileReader(file_user);
            BufferedReader bufferedReader_user = new BufferedReader(fileReader_user);
            String line_user;
            String business_id;
            JSONObject jObj;
            JSONArray obj;
            JSONObject obj_attrib;
            PreparedStatement statement;
            int tester = 0;
            boolean check_the_attribute;

            Class.forName("oracle.jdbc.driver.OracleDriver");
            Connection con = null;
            con = DriverManager.getConnection(DBURL, DBUSER, DBPASS);

            big_attrib_tester.clear();

            while ((line_user = bufferedReader_user.readLine()) != null) {
                tester++;
                jObj = (JSONObject) new JSONTokener(line_user)
                        .nextValue();

                obj_attrib = jObj.getJSONObject("attributes");

                business_id = jObj.getString("business_id");
                String attributes = "000000000000000000000000000000000000000000000000000000000000"
                        + "000000000000000000000000000000000000000";
                obj = jObj.getJSONArray("categories");

                test_attrib_list.clear();
                test_attrib_true.clear();

                parse_jObj(obj_attrib, "", 0);

                int check_aattribute_size = big_attrib_tester.size() + test_attrib_list.size() - attributes.length();

                if (check_aattribute_size > 0) {
                    for (int i = 0; i < check_aattribute_size; i++) {
                        attributes += "0";
                    }
                }

                check_the_attribute = false;
                StringBuffer bf = new StringBuffer(attributes);
                if (big_attrib_tester.size() == 0) {
                    for (int i = 0; i < test_attrib_list.size(); i++) {
                        big_attrib_tester.add(test_attrib_list.get(i));
                        bf.setCharAt(i, '1');
                    }
                } else {
                    for (int i = 0; i < test_attrib_list.size(); i++) {
                        if (!(big_attrib_tester.contains(test_attrib_list.get(i)))) {
                            big_attrib_tester.add(test_attrib_list.get(i));
                            if (test_attrib_true.get(i) != "false") {
                                bf.setCharAt(big_attrib_tester.size(), '1');
                                if (big_attrib_tester.size() > 90) {
                                    check_the_attribute = true;
                                }
                            }
                        } else if (test_attrib_true.get(i) != "false") {
                            bf.setCharAt(big_attrib_tester.indexOf(test_attrib_list.get(i)), '1');
                            if (big_attrib_tester.indexOf(test_attrib_list.get(i)) > 90) {
                                check_the_attribute = true;
                            }
                        }
                    }
                }

                attributes = bf.toString();

                statement = con.prepareStatement("UPDATE BUSINESS SET ATTRIB=? WHERE BID = ?");
                statement.setString(1, attributes);
                statement.setString(2, business_id);
                statement.executeUpdate();
                statement.close();

//                if (check_the_attribute) {
//                    System.out.println(tester);
//                    System.out.println(attributes);
//
//                    for (int i = 0; i < test_attrib_list.size(); i++)
//                        System.out.println(test_attrib_list.get(i) + ":" + test_attrib_true.get(i));
//                    System.out.println("------------------");
//                }
//
                ArrayList<String> list = new ArrayList<String>();
                if (obj != null) {
                    int len = obj.length();
                    for (int i = 0; i < len; i++) {
                        list.add(obj.get(i).toString());
                    }
                    for (int i = 0; i < list.size(); i++) {
                        name = list.get(i);
                        if (!valid_maincategories.contains(name)) {
                            statement = con.prepareStatement("INSERT INTO BUSINESS_SUB_CATEGORY(BID, B_SUB_CATEGORY) VALUES(?, ?)");
                            statement.setString(1, business_id);
                            statement.setString(2, name);
                        } else {
                            statement = con.prepareStatement("INSERT INTO BUSINESS_CATEGORY(BID, B_CATEGORY) VALUES(?, ?)");
                            statement.setString(1, business_id);
                            statement.setString(2, name);
                        }
                        statement.executeUpdate();
                        statement.close();
                    }
                }
            }

            for (int i = 0; i < big_attrib_tester.size(); i++) {
                statement = con.prepareStatement("INSERT INTO NATIVE_ATTRIBUTE(ATTRIB) VALUES(?)");
                statement.setString(1,big_attrib_tester.get(i));
                statement.executeUpdate();
                statement.close();
            }
            for (int i = 0; i < Main_categories.length; i++) {
                statement = con.prepareStatement("INSERT INTO NATIVE_CATEGORY(CATEGORY) VALUES(?)");
                statement.setString(1, Main_categories[i]);
                statement.executeUpdate();
                statement.close();
            }

            fileReader_user.close();
            con.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException ex) {
            Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void parse_reviews() {

        int funny;
        int cool;
        int useful;
        String business_id;
        String user_id;
        String review_id;
        String date;
        int rating;
        String text;

        try {
            File file_user = new File(yelp_review_file);
            FileReader fileReader_user = new FileReader(file_user);
            BufferedReader bufferedReader_user = new BufferedReader(fileReader_user);
            String line_user;
            PreparedStatement statement = null;
            JSONObject jObj;
            JSONObject obj;

            Class.forName("oracle.jdbc.driver.OracleDriver");
            Connection con = null;
            con = DriverManager.getConnection(DBURL, DBUSER, DBPASS);
            int batch_updates = 0;
            int tester = 0;

            while ((line_user = bufferedReader_user.readLine()) != null) {

                batch_updates++;
                tester++;
                jObj = (JSONObject) new JSONTokener(line_user)
                        .nextValue();

                obj = jObj.getJSONObject("votes");

                funny = obj.getInt("funny");
                cool = obj.getInt("cool");
                useful = obj.getInt("useful");
                business_id = jObj.getString("business_id");
                user_id = jObj.getString("user_id");
                review_id = jObj.getString("review_id");
                date = jObj.getString("date");
                rating = jObj.getInt("stars");
                text = jObj.getString("text");

                statement = con.prepareStatement("INSERT INTO REVIEWS(REV_ID, RATING, USER_ID, PUBLISH_DATE, REVIEW_TEXT,"
                        + "BID, VOTES_FUNNY, VOTES_COOL, VOTES_USEFUL) VALUES(?,?,?,?,?,?,?,?,?)");
                statement.setString(1, review_id);
                statement.setInt(2, rating);
                statement.setString(3, user_id);
                statement.setDate(4, java.sql.Date.valueOf(date));
                //statement.setTimestamp(4, Timestamp.valueOf(date + " 00:00:00"));
                statement.setString(5, text);
                statement.setString(6, business_id);
                statement.setInt(7, funny);
                statement.setInt(8, cool);
                statement.setInt(9, useful);
                statement.executeUpdate();
                statement.close();
            }
            fileReader_user.close();
            con.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, ex);
        } catch (JSONException ex) {
            Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void parse_business() {

        String full_address;

        // TODO code application logic here
        try {
            File file_user = new File(yelp_business_file);
            FileReader fileReader_user = new FileReader(file_user);
            BufferedReader bufferedReader_user = new BufferedReader(fileReader_user);
            String line_user;

            Class.forName("oracle.jdbc.driver.OracleDriver");

            Connection con = null;

            con = DriverManager.getConnection(DBURL, DBUSER, DBPASS);

            while ((line_user = bufferedReader_user.readLine()) != null) {
                // TODO code application logic here
                try {

                    JSONObject jObj_business = (JSONObject) new JSONTokener(line_user)
                            .nextValue();
                    PreparedStatement statement = null;
                    boolean is_open = false;

                    String business_id = jObj_business.getString("business_id");
                    String business_name = jObj_business.getString("name");
                    full_address = jObj_business.getString("full_address");
                    is_open = jObj_business.getBoolean("open");
                    Double rating = jObj_business.getDouble("stars");
                    Integer Rev_Count = jObj_business.getInt("review_count");
                    // System.out.println(jObj_business.getString("hours"));
                    JSONObject hours = jObj_business.getJSONObject("hours");
                    ArrayList<String> list = new ArrayList<String>();
                    JSONArray cats = (JSONArray) jObj_business.get("categories");


                    String[] sample = {};
                    String sub_category = "";
                    String main_category = "";
                    String name = "";

                    Iterator keys = hours.keys();
                    String days = "";
                    JSONObject time = new JSONObject();
                    while (keys.hasNext()) {
                        // loop to get the dynamic key
                        days = (String) keys.next();

                        // get the value of the dynamic key
                        time = hours.getJSONObject(days);

                    }
                    //BufferedReader bufferedReader_hours = new BufferedReader(hours);
                    String[] address_split = full_address.split("\\n");
                    String check_pin = address_split[address_split.length - 1];
                    String street = "";
                    String pin = null;
                    String state = null;
                    String city = null;

                    // Parsing the rest
                    if (address_split.length > 1) {
                        street += full_address.substring(0, full_address.length() - check_pin.length());
                    }

                    if (check_pin.contains(",")) {
                        int i;
                        for (i = 0; i < check_pin.length(); i++) {
                            if (check_pin.charAt(i) == ',') {
                                break;
                            }
                        }
                        //System.out.println(check_pin);
                        city = check_pin.substring(0, i);
                        if ((i + 1) < check_pin.length()) {
                            String my_check_state = check_pin.substring(i + 2, check_pin.length());
                            String[] check_parts = my_check_state.split(" ");

                            if (check_parts.length < 2) {
                                state = my_check_state;
                            } else //System.out.println(my_check_state);
                                if (check_parts[check_parts.length - 1].length() > 4) {
                                    pin = check_parts[check_parts.length - 1];
                                    state = "";
                                    for (int k = 0; k < check_parts.length - 1; k++) {
                                        state += check_parts[k];
                                    }
                                } else {
                                    pin = "";
                                    state = "";
                                    //System.out.println(my_check_state);
                                    for (int k = 0; k < check_parts.length; k++) {
                                        if (k != 0) {
                                            if (check_parts[k].length() == 3) {
                                                pin += check_parts[k];
                                                if (k != check_parts.length - 1) {
                                                    pin += " ";
                                                }
                                                continue;
                                            }
                                        }
                                        state += check_parts[k];
                                    }
                                }
                        }

                    } else {
                        String[] check_parts = check_pin.split(" ");
                        city = "";
                        state = "";
                        pin = "";
                        //System.out.println(check_pin);
                        for (int i = 0; i < check_parts.length; i++) {
                            String check_more = check_parts[i];
                            boolean is_num = false;
                            boolean is_caps = false;
                            boolean is_small = false;
                            for (int j = 0; j < check_more.length(); j++) {
                                if (Character.isUpperCase(check_more.charAt(j))) {
                                    is_caps = true;
                                }
                                if (Character.isDigit(check_more.charAt(j))) {
                                    is_num = true;
                                }
                                if (Character.isLowerCase(check_more.charAt(j))) {
                                    is_small = true;
                                }
                            }
                            if (is_num) {
                                if (!is_small) {
                                    pin += check_more;
                                    if (i != (check_parts.length - 1)) {
                                        pin += " ";
                                    }
                                }
                            } else {
                                if (i != 0) {
                                    city += " ";
                                }
                                city += check_more;
                            }
                        }
                    }

                    statement = con.prepareStatement("INSERT INTO BUSINESS(BID, B_NAME, STREET, CITY, STATE_NM, PIN, REVIEW_CNT, RATING, B_OPEN, MONDAY_TIME_OPEN,  MONDAY_TIME_CLOSE,"
                            + "TUESDAY_TIME_OPEN, TUESDAY_TIME_CLOSE, WEDNESDAY_TIME_OPEN, WEDNESDAY_TIME_CLOSE, THURSDAY_TIME_OPEN, THURSDAY_TIME_CLOSE,"
                            + "FRIDAY_TIME_OPEN, FRIDAY_TIME_CLOSE, SATURDAY_TIME_OPEN, SATURDAY_TIME_CLOSE, SUNDAY_TIME_OPEN, SUNDAY_TIME_CLOSE"
                            + ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

                    statement.setString(1, business_id);
                    statement.setString(2, business_name);
                    statement.setString(3, street);
                    statement.setString(4, city);
                    statement.setString(5, state);
                    statement.setString(6, pin);
                    statement.setInt(7, Rev_Count);
                    statement.setDouble(8, rating);
                    if (is_open) {
                        statement.setString(9, "true");
                    } else {
                        statement.setString(9, "false");
                    }
                    String[] weekdays = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};
                    for (int j = 0; j < weekdays.length; j++) {
                        int timer1 = j * 2 + 10;
                        int timer2 = j * 2 + 11;
                        boolean check_timestamp = false;
                        if (hours.has(weekdays[j])) {
                            JSONObject day = hours.getJSONObject(weekdays[j]);
                            if (day.has("open")) {
                                String open = day.getString("open");
                                statement.setTimestamp(timer1, Timestamp.valueOf("0001-01-01 " + open + ":00"));
                                check_timestamp = true;
                            }
                            if (day.has("close")) {
                                String close = day.getString("close");
                                statement.setTimestamp(timer2, Timestamp.valueOf("0001-01-01 " + close + ":00"));
                                check_timestamp = true;
                            }
                        }
                        if (!check_timestamp) {
                            statement.setTimestamp(timer1, null);
                            statement.setTimestamp(timer2, null);
                        }
                    }

                    statement.executeUpdate();
                    statement.close();
                    //
                } catch (JSONException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

            }
            System.out.println("reached\n");
            con.close();
            fileReader_user.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public static void parse_users() {

        // TODO code application logic here
        try {
            File file_user = new File(yelp_user_file);
            FileReader fileReader_user = new FileReader(file_user);
            BufferedReader bufferedReader_user = new BufferedReader(fileReader_user);
            String line_user;
            String user_id, user_name;
            String date;
            int review_count;
            int numberOfFriends;
            int cool;
            int funny;
            int useful;
            JSONObject obj;
            Double averageStars;
            PreparedStatement statement;
            int tester = 0;

            Class.forName("oracle.jdbc.driver.OracleDriver");

            Connection con = null;

            con = DriverManager.getConnection(DBURL, DBUSER, DBPASS);

            while ((line_user = bufferedReader_user.readLine()) != null) {
                // TODO code application logic here
                tester++;
                try {

                    JSONObject jObj_user = (JSONObject) new JSONTokener(line_user)
                            .nextValue();

                    user_id = jObj_user.getString("user_id");
                    user_name = jObj_user.getString("name");
                    date = jObj_user.getString("yelping_since");
                    review_count = jObj_user.getInt("review_count");
                    DecimalFormat df = new DecimalFormat("#.##");
                    averageStars = jObj_user.getDouble("average_stars");
                    averageStars =  Double.parseDouble(df.format(averageStars));
                    JSONArray numOfFriends = jObj_user.getJSONArray("friends");
                    numberOfFriends = numOfFriends.length();
                    obj = jObj_user.getJSONObject("votes");

                    funny = obj.getInt("funny");
                    cool = obj.getInt("cool");
                    useful = obj.getInt("useful");
                    //System.out.println(user_name + "," + user_id);
                    user_id = formatString(user_id);
                    user_name = formatString(user_name);

                    statement = con.prepareStatement("INSERT INTO YELP_USER (USER_ID, USER_NAME, MEMBER_SINCE, " +
                            "REVIEW_COUNT, NUMBER_OF_FRIENDS, AVERAGE_STARS, VOTE_FUNNY, VOTE_COOL, VOTE_USEFUL)" +
                            " VALUES(?,?,?,?,?,?,?,?,?)");
                    statement.setString(1, user_id);
                    statement.setString(2, user_name);
                    statement.setDate(3, java.sql.Date.valueOf(date + "-01"));
                    statement.setInt(4, review_count);
                    statement.setInt(5, numberOfFriends);
                    statement.setDouble(6, averageStars);
                    statement.setInt(7, funny);
                    statement.setInt(8, cool);
                    statement.setInt(9, useful);
                    statement.executeUpdate();
                    statement.close();

                } catch (JSONException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                if (tester % 10000 == 0) {
                    System.out.println(tester);
                }

            }
            con.close();
            fileReader_user.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static String formatString(String str) {
        String strSingleQuotation = "\'||chr(39)||\'";
        StringBuffer strReturn = new StringBuffer();
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) == '\'') {
                strReturn.append(strSingleQuotation);
            } else {
                strReturn.append(str.charAt(i));
            }
        }
        return strReturn.toString();
    }

    public static void parse_jObj(JSONObject jObj, String check_back, int x) throws JSONException {
        Iterator<?> keys = jObj.keys();
        while (keys.hasNext()) {
            String key = (String) keys.next();
            Object check_me = jObj.get(key);
            if (check_me instanceof JSONObject) {
                if (x == 0) {
                    parse_jObj(jObj.getJSONObject(key), key, 1);
                } else {
                    parse_jObj(jObj.getJSONObject(key), check_back + "_" + key, 1);
                }
            } else if (check_me instanceof JSONArray) {

            } else if (check_me instanceof Boolean) {
                if (x == 0) {
                    test_attrib_list.add(key);
                } else {
                    test_attrib_list.add(check_back + "_" + key);
                }
                if (jObj.getBoolean(key)) {
                    test_attrib_true.add("true");
                } else {
                    test_attrib_true.add("false");
                }
            } else {
                if (x == 0) {
                    test_attrib_list.add(key + "_" + check_me.toString());
                } else {
                    test_attrib_list.add(check_back + "_" + key + "_" + check_me.toString());
                }
                test_attrib_true.add("none");
            }
        }
    }
}
