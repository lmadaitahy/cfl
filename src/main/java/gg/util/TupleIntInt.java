package gg.util;

public final class TupleIntInt {

    public int f0, f1;

    public TupleIntInt() {}

    public TupleIntInt(int f0, int f1) {
        this.f0 = f0;
        this.f1 = f1;
    }



    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TupleIntInt that = (TupleIntInt) o;

        if (f0 != that.f0) return false;
        return f1 == that.f1;

    }

    @Override
    public int hashCode() {
        int result = f0;
        result = 31 * result + f1;
        return result;
    }

    @Override
    public String toString() {
        return "TupleIntInt{" +
                "f0=" + f0 +
                ", f1=" + f1 +
                '}';
    }


    public static TupleIntInt of(int f0, int f1) {
        return new TupleIntInt(f0, f1);
    }
}
