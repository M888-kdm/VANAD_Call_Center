import java.util.LinkedList;

public class test {

    public static void main(String[] args){
        LinkedList<Integer> list = new LinkedList<>();
        list.add(5);
        list.add(6);

        LinkedList<Integer> list2 = list;

        list2.add(7);

        for(int i=0; i<list.size(); i++)
            System.out.println(list.get(i));
    }

}
