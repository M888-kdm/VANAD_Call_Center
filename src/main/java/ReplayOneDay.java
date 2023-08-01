import java.io.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import umontreal.ssj.simevents.*;


public class ReplayOneDay {

    private static int MAX_QUEUE_SIZE = 100;
    String outputFilePath = "src/main/resources/dataset.csv";
    PrintWriter pw;

    //Contains the length of queues
    private int[] array_queue_length =  new int[27];
    private double[] array_LES = new double[27];
    private LinkedList[] array_Avg_LES = new LinkedList[27];
    private LinkedList[][] array_AvgC_LES = new LinkedList[27][MAX_QUEUE_SIZE];
    private double[][] array_WAvgC_LES = new double[27][MAX_QUEUE_SIZE];
    private int nb_servers = 0;
    private ArrayList<Customer> served_customer = new ArrayList<>();
    private ArrayList<Customer> abandon_customer = new ArrayList<>();

    public ReplayOneDay() {
        File csvOutputFile = new File(outputFilePath);
        for (int i=0; i<27; i++){
            array_Avg_LES[i] = new LinkedList<Double>();
            for (int j=0; j<MAX_QUEUE_SIZE; j++){
                array_AvgC_LES[i][j] = new LinkedList<Double>();
            }
        }
        try  {
            pw = new PrintWriter(new FileWriter(csvOutputFile, true));
        }catch (Exception e){
        }
//        String[] headers = new String[]{"customer_type", "file_length_at_arrival", "arrival_time", "nb_server", "les",
//                "avg_les", "avgC_les", "wAvgC_les", "len1", "len2", "len3", "len4", "len5", "waiting_time"};
//        String header_data = convertToCsv(headers);
//        pw.write(header_data);
//        pw.write("\n");
    }

    public void createCustomerOfTheDay(String file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        br.readLine();
        String read_line = br.readLine();
        while (read_line != null){
            Customer cust = new Customer();
            String[] elements =  read_line.split(",");

            cust.setArrival_time(getTime(elements[0]));
            cust.setType(Integer.parseInt(elements[7]));
            cust.setWaiting_time(getWaitingTime(elements[0], elements[3], elements[6]));

            if (elements[3].compareTo("NULL") == 0) {
                cust.setIs_served(false);
            }
            else {
                cust.setService_time(getTime(elements[6]) - getTime(elements[3]));
            }

            //On programme la rentree du cust dans la file
            new Arrival(cust).schedule(cust.getArrival_time());
            read_line = br.readLine();
        }
    }

    public double getTime(String s) {
        String s1 = s.split(" ")[1];
        String[] time = s1.split(":");
        double converted_time = Integer.parseInt(time[0])*3600 + Integer.parseInt(time[1])*60 + Integer.parseInt(time[2]) - 8*3600;
        if(converted_time < 0)
            System.out.println(s);
        return converted_time;
    }

    public double getWaitingTime(String arrival, String answered, String hangup){
        if (answered.compareTo("NULL") != 0) {
            return getTime(answered) - getTime(arrival);
        } else {
            return getTime(hangup) - getTime(arrival);
        }
    }


    public class Arrival extends Event {
        Customer cust;
        int N_AvgLES = 10;
        int cust_type;
        int file_length_at_arrival;
        Double cust_waiting_time;
        LinkedList<Double> cust_AvgLES_list = new LinkedList<>();
        LinkedList<Double> cust_AvgCLES_list = new LinkedList<>();

        public Arrival(Customer cust){
            this.cust = cust;
        }
        public Double computeAvgLES(LinkedList<Double> cust_waiting_time_list ) {
            double tmp = 0;
            int index = 0;

            if (cust_waiting_time_list.isEmpty()) {
                return 0.0;
            }
            else {
                for (int i=0; i<cust_waiting_time_list.size(); i++) {
                    index++;
                    if(i==N_AvgLES) break;
                    tmp += cust_waiting_time_list.get(i);
                }
                return tmp/index;
            }
        }

        public void actions() {
            cust_type = cust.getType();

            // Incrémenter le nombre de cust de ce type (array_queue_length)
            array_queue_length[cust_type] ++;

            cust_waiting_time = cust.getWaiting_time();
            cust_AvgLES_list = array_Avg_LES[cust_type];

            // Initialiser le nombre de cust du meme type trouve dans la file
            cust.setFiles_length(array_queue_length);

            // Initialisation des predicteurs(LES, AvgLES, AvgCLES, WAvgCLES)

            //LES predictor
            cust.setLES(array_LES[cust_type]);

            //AvgLES predictor
            cust.setAvg_LES(computeAvgLES(cust_AvgLES_list));

//          AvgCLES predictor
            file_length_at_arrival = cust.getFiles_length()[cust_type];

            cust_AvgCLES_list = array_AvgC_LES[cust_type][file_length_at_arrival];
            cust.setAvgC_LES(computeAvgLES(cust_AvgCLES_list));

//          WavgCLES
            cust.setWAvgC_LES(array_WAvgC_LES[cust_type][file_length_at_arrival]);

//          Initialiser nb_servers(le nombre de serveurs occupés)
            cust.setNb_server(nb_servers);

//          scheduler son depart(abandon ou servis) de la file dans waiting_time;
            new Departure(cust).schedule(cust_waiting_time);
        }
    }

    public class Departure extends Event {
        Customer cust;

        public Departure(Customer cust){
            this.cust = cust;
        }

        public void actions() {
            int type = cust.getType();

            // Decrementer le nb cust de ce type  (array_queue_length)
            array_queue_length[type]--;

            double waiting_time = cust.getWaiting_time();

            // Mettre à jour les données utilisées par les prédicteurs

            if(cust.isIs_served()){
                // Mise à jour du prédicteur LES
                array_LES[type] = waiting_time;

                // Mise à jour du prédicteur Avg_LES
                array_Avg_LES[type].addFirst(waiting_time);

                // Mise à jour du prédicteur AvgC_LES
                int file_length_at_arrival = cust.getFiles_length()[type];

                LinkedList<Double> matching_list = array_AvgC_LES[type][file_length_at_arrival];

                if(matching_list.size() == MAX_QUEUE_SIZE){
                    matching_list.removeLast();
                }
                matching_list.addFirst(waiting_time);

                // Mise à jour du prédicteur WAvgC_LES
                array_WAvgC_LES[type][file_length_at_arrival] *= 0.8;
                array_WAvgC_LES[type][file_length_at_arrival] += 0.2 * cust.getWaiting_time();
            }

            // Si depart == debutService alors incrementer nb_servers
            if(cust.isIs_served()){
                nb_servers += 1;
                // scheduler la fin de service
                new CallCompletion(cust).schedule(cust.getService_time());
            }

        }
    }

    public class CallCompletion extends Event {
        Customer cust;

        public CallCompletion(Customer cust){this.cust = cust;}

        public void actions(){
            try{
                writeToFile(this.cust);
            }
            catch (Exception e){
            }
            nb_servers--;
        }

    }

    public String convertToCsv(String[] data){
        return Stream.of(data).collect(Collectors.joining(","));
    }

    private String convertIntegerToString(int i){
        return Integer.toString(i);
    }

    private String convertDoubleToString(double d){
        DecimalFormat decimalFormat = new DecimalFormat("#.##");
        return decimalFormat.format(d);
    }

    public void writeToFile(Customer cust) throws IOException{
        List<String> data = new ArrayList<>();

        data.add(convertIntegerToString(cust.getType()));
        data.add(convertIntegerToString(cust.getFiles_length()[cust.getType()]));
        data.add(convertDoubleToString(cust.getArrival_time()));
        data.add(convertIntegerToString(cust.getNb_server()));
        data.add(convertDoubleToString(cust.getLES()));
        data.add(convertDoubleToString(cust.getAvg_LES()));
        data.add(convertDoubleToString(cust.getAvgC_LES()));
        data.add(convertDoubleToString(cust.getWAvgC_LES()));
        data.add(convertIntegerToString(cust.getFiles_length()[2]));
        data.add(convertIntegerToString(cust.getFiles_length()[5]));
        data.add(convertIntegerToString(cust.getFiles_length()[9]));
        data.add(convertIntegerToString(cust.getFiles_length()[11]));
        data.add(convertIntegerToString(cust.getFiles_length()[20]));
        data.add(convertDoubleToString(cust.getWaiting_time()));

        String[] csv_data = data.toArray(new String[0]);
        String file_data = convertToCsv(csv_data);
        pw.write(file_data);
        pw.write("\n");
    }

    public void simulateOneDay(String filepath) throws  IOException{
        Sim.init();
        String inputFilePath = filepath;
        this.createCustomerOfTheDay(inputFilePath);

        Sim.start();
    }

    public static void main(String[] args) throws IOException {
        File directory = new File("src/main/resources/VANAD_data_processed");
        File[] files = directory.listFiles();
        for(File file: files){
            ReplayOneDay replayOneDay = new ReplayOneDay();
            replayOneDay.simulateOneDay(file.getPath());
        }
    }

}
