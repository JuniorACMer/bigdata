/**
 * @author inspur
 * @version 1.0
 * @date 2020/4/14 11:40
 */
public class Test {
    public static void main(String[] args) {
        int a = 10;
        assert a > 0 : "something goes wrong here, a cannot be less than 0";
        System.out.println(a);
    }
}
