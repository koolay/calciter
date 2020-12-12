package calciter;

/**
 * Unchecked exception that Samza throws when something goes wrong.
 */
public class SamzaException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public SamzaException() {
    super();
  }

  public SamzaException(String s, Throwable t) {
    super(s, t);
  }

  public SamzaException(String s) {
    super(s);
  }

  public SamzaException(Throwable t) {
    super(t);
  }
}
