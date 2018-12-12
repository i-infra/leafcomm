import numpy as np


def fix_iq_imbalance(x):
    # remove DC and save input power
    z = x - np.mean(x)
    p_in = np.var(z)

    # scale Q to have unit amplitude (remember we're assuming a single input tone)
    Q_amp = np.sqrt(2 * np.mean(x.imag**2))
    z /= Q_amp

    I, Q = z.real, z.imag

    alpha_est = np.sqrt(2 * np.mean(I**2))
    sin_phi_est = (2 / alpha_est) * np.mean(I * Q)
    cos_phi_est = np.sqrt(1 - sin_phi_est**2)

    I_new_p = (1 / alpha_est) * I
    Q_new_p = (-sin_phi_est / alpha_est) * I + Q

    y = (I_new_p + 1j * Q_new_p) / cos_phi_est

    print('phase error:', np.arccos(cos_phi_est) * 360 / 2 / np.pi, 'degrees')
    print('amplitude error:', 20 * np.log10(alpha_est), 'dB')

    return y * np.sqrt(p_in / np.var(y))
