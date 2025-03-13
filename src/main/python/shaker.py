from gpiozero import LED

class Shaker:
    def __init__(self, gpio_available: bool):
        if gpio_available:
            self.led = LED(17)

    def shake(self):
        if self.led is not None:
            self.led.blink(on_time=0.1, off_time=0.1, n=10)
        else:
            print("Shaking not available")
