package com.kaikeba.hadoop.secondarysort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//根据输入文件格式，定义JavaBean，作为MR时，Map的输出key类型；要求此类可序列化、可比较
public class Person implements WritableComparable<Person> {
    private String name;
    private int age;
    private int salary;

    public Person() {
    }

    public Person(String name, int age, int salary) {
        //super();
        this.name = name;
        this.age = age;
        this.salary = salary;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getSalary() {
        return salary;
    }

    public void setSalary(int salary) {
        this.salary = salary;
    }

    @Override
    public String toString() {
        return this.salary + "  " + this.age + "    " + this.name;
    }

    //两个Person对象的比较规则：①先比较salary，高的排序在前；②若相同，age小的在前
    public int compareTo(Person other) {
        int compareResult= this.salary - other.salary;
        if(compareResult != 0) {//若两个人工资不同
            //工资降序排序；即工资高的排在前边
            return -compareResult;
        } else {//若工资相同
            //年龄升序排序；即年龄小的排在前边
            return this.age - other.age;
        }
    }

    //序列化，将NewKey转化成使用流传送的二进制
    public void write(DataOutput dataOutput) throws IOException {
        //注意：①使用正确的write方法；②记住此时的序列化的顺序，name、age、salary
        dataOutput.writeUTF(name);
        dataOutput.writeInt(age);
        dataOutput.writeInt(salary);
    }

    //使用in读字段的顺序，要与write方法中写的顺序保持一致：name、age、salary
    public void readFields(DataInput dataInput) throws IOException {
        //read string
        //注意：①使用正确的read方法；②读取顺序与write()中序列化的顺序保持一致
        this.name = dataInput.readUTF();
        this.age = dataInput.readInt();
        this.salary = dataInput.readInt();
    }
}
