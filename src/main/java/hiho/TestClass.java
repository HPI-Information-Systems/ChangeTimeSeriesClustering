package hiho;

public class TestClass {

    private int age;
    private String name;

    public TestClass(int age, String name) {
        this.age = age;
        this.name = name;
    }

    @Override
    public String toString() {
        return "hiho.TestClass{" +
                "age=" + age +
                ", name='" + name + '\'' +
                '}';
    }
}
