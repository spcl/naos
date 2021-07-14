/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * Usage example. 
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */
package ch.ethz.microperf;

import ch.ethz.microperf.datastructures.PointFloat;
import ch.ethz.naos.skyway.Skyway;

import java.nio.ByteBuffer;

public class TestSkyway {

    static int val = 0;


    static class PersonD1 {
       // String name;
        int age;
        float grade;

        public PersonD1( ) {
        //    this.name = new String("Mike" + val);
            this.age = val++;
            this.grade = (float)(val+10);
          //  this.age2 = new Float((val+10));
        }
    }

    static class PersonD2 {
       // String name;
        Integer age;
        float grade;
      //  Float age2;

        public PersonD2( ) {
        //    this.name = new String("Mike" + val);
            this.age = new Integer(val++);
            this.grade = (float)(val+10);
          //  this.age2 = new Float((val+10));
        }
    }

    static class PersonD3 {
       // String name;
        Integer age;
        Float age2;

        public PersonD3( ) {
        //    this.name = new String("Mike" + val);
            this.age = new Integer(val++);
            this.age2 = new Float((val+10));
        }
    }

    static class PersonD4  {
       // String name;
        PersonD2 age;
      //  Float age2;

        public PersonD4 ( ) {
        //    this.name = new String("Mike" + val);
            this.age = new PersonD2();
          //  this.age2 = new Float((val+10));
        }
    }


    public static void main(String[] args) throws Exception {
 
        int size = args.length > 0 ? Integer.parseInt(args[0]) : 1024;

        Skyway sk = new Skyway();

        System.out.printf("Test: 1\n");
        { // test 1 depth 1
            PersonD1[] people = new PersonD1[size];

            for(int i=0; i<size; i++){
                people[i] = new PersonD1();
            }

            for(int i=0; i<10; i++){
                long bfs = sk.sizeOfObjectTest(people, true); // bfs
                long dfs = sk.sizeOfObjectTest(people, false); // Dfs
            }
        }

        System.out.printf("\nTest: 1-1\n");
        { // test 1 depth 1
            PersonD2[] people = new PersonD2[size];

            for(int i=0; i<size; i++){
                people[i] = new PersonD2();
            }

            for(int i=0; i<10; i++){
                long bfs = sk.sizeOfObjectTest(people, true); // bfs
                long dfs = sk.sizeOfObjectTest(people, false); // Dfs
            }
        }

        System.out.printf("\nTest: 1-2\n");
        { // test 1 depth 1
            PersonD3[] people = new PersonD3[size];

            for(int i=0; i<size; i++){
                people[i] = new PersonD3();
            }

            for(int i=0; i<10; i++){
                long bfs = sk.sizeOfObjectTest(people, true); // bfs
                long dfs = sk.sizeOfObjectTest(people, false); // Dfs
            }
        }

        System.out.printf("\nTest: 1-1-1\n");
        { // test 1 depth 1
            PersonD4[] people = new PersonD4[size];

            for(int i=0; i<size; i++){
                people[i] = new PersonD4();
            }

            for(int i=0; i<10; i++){
                long bfs = sk.sizeOfObjectTest(people, true); // bfs
                long dfs = sk.sizeOfObjectTest(people, false); // Dfs
            }
        }
 
 


        sk.registerClass(PointFloat.class,0);
        sk.registerClass(PointFloat[].class,1);

 
        int n = 10;
        PointFloat[] arr = new PointFloat[n];
        for(int i=0;i<5;i++){
            arr[i] = new PointFloat(i,i);
        }
        long objsize = sk.sizeOfObject(arr);
        byte[] futurebuf = new byte[(int)objsize];

        byte[] bb = sk.writeObject(arr);
        System.out.printf("Serialization is done. size: %d\n",bb.length);
        PointFloat[] arr2 = (PointFloat[])sk.readObject(bb);
        System.out.printf("Serialization is done. size: %d\n",bb.length);
        objsize = sk.sizeOfObject(arr2);
        for(int i=0;i<arr2.length;i++){
            if(arr2[i] == null){
                System.out.println(i + "is null");
            }else{
                System.out.println(arr2[i]);
            }
        }

     /*   
        sk.writeObject(arr,futurebuf);
        System.out.printf("Serialization is done. size: %d\n",bb.length);
        PointFloat[] arr2 = (PointFloat[])sk.readObject(futurebuf);
        System.out.printf("Serialization is done. size: %d\n",bb.length);
        */
 

    }
}
