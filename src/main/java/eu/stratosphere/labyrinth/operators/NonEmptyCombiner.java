package eu.stratosphere.labyrinth.operators;

/**
 * Don't forget to either set the parallelism to 1, or put an Or with para 1 after this.
 *
 * Abban kulonbozik a NonEmpty-tol, hogy nem kuld olyankor, ha nem jott be elem
 */
public class NonEmptyCombiner<T> extends BagOperator<T, Boolean> {

    private static final int closedNum = -1000;

    private int num = closedNum;
    private boolean sent = false;

    @Override
    public void openOutBag() {
        super.openOutBag();
        assert num == closedNum;
        num = 0;
        sent = false;
    }

    @Override
    public void pushInElement(T e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        num++;
        if (!sent) {
            out.collectElement(true);
            sent = true;
        }
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
//        if (num == 0) {
//            out.collectElement(false);
//        }
        num = closedNum;
        out.closeBag();
    }
}
