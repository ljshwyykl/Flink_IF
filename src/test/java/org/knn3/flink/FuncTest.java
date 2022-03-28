package org.knn3.flink;

import org.junit.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Optional;

public class FuncTest {
    @Test
    public void subtract() throws IOException {
        System.out.println("add");
        BigInteger  result =  Optional.ofNullable(new BigInteger("5")).orElseGet(() -> new BigInteger("0")).subtract(new BigInteger("1"));
        System.out.println("result:"+result);

    }

    @Test
    public void add() throws IOException {
        BigInteger  result =  Optional.ofNullable(new BigInteger("0")).map(x -> x.add(new BigInteger("2"))).orElseGet(() -> new BigInteger("" + new BigInteger("3")));

        System.out.println("result:"+result);

    }



}
